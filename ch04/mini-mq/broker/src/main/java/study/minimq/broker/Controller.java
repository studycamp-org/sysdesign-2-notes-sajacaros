package study.minimq.broker;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import study.minimq.broker.raft.RaftNode;
import study.minimq.client.MiniMqHttp;
import study.minimq.common.BrokerInfo;
import study.minimq.common.ClusterMetadata;
import study.minimq.common.PartitionMetadata;
import study.minimq.common.TopicInfo;
import study.minimq.common.TopicMetadata;
import study.minimq.common.Topics;

/**
 * 컨트롤러가 하는 일 (정리.md 「컨트롤러 정리」). Raft 리더인 브로커에서만 실제로 동작한다.
 * <ul>
 *   <li>사본 분산 계획: 토픽을 만들면 파티션마다 사본을 서로 다른 브로커에 둔다(리더도 고르게 퍼진다)</li>
 *   <li>서비스 탐색: 박동 응답이 끊긴 브로커를 죽음으로 표시한다</li>
 *   <li>장애 복구 (그림 4.28): 죽은 브로커가 리더였던 파티션은 ISR 에서 새 리더를 뽑고 leader epoch 를 올린다.
 *       ISR 이 모두 죽었으면 리더 없음(-1)으로 두고, ISR 의 마지막 브로커가 돌아오기를 기다린다</li>
 *   <li>ISR 변경: 파티션 리더의 요청을 받아 메타데이터에 반영한다</li>
 * </ul>
 */
@Component
public class Controller {
    private final RaftNode raft;
    private final BrokerProperties props;

    public Controller(RaftNode raft, BrokerProperties props) {
        this.raft = raft;
        this.props = props;
    }

    // ------------------------------------------------------------------ 토픽

    public TopicInfo createTopic(TopicInfo req) {
        if (req.name() == null || !req.name().matches("[A-Za-z0-9._-]+")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "토픽 이름은 영문·숫자·._- 만");
        }
        if (req.partitions() < 1) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "파티션은 1개 이상");
        if (!raft.isLeader()) return forwardToController("/topics", req, TopicInfo.class);
        return create(req);
    }

    private TopicInfo create(TopicInfo req) {
        TopicMetadata existing = raft.accepted().topics().get(req.name());
        if (existing != null) return info(existing);

        List<Integer> alive = raft.accepted().brokers().values().stream()
                .filter(BrokerInfo::alive).map(BrokerInfo::id).sorted().toList();
        int rf = Math.min(req.replicationFactor() > 0 ? req.replicationFactor() : 3, alive.size());
        int minIsr = Math.min(req.minInsyncReplicas() > 0 ? req.minInsyncReplicas() : 1, rf);
        List<PartitionMetadata> parts = new ArrayList<>();
        for (int p = 0; p < req.partitions(); p++) {
            List<Integer> replicas = new ArrayList<>();
            for (int i = 0; i < rf; i++) replicas.add(alive.get((p + i) % alive.size())); // 같은 파티션의 사본은 서로 다른 브로커
            parts.add(new PartitionMetadata(p, replicas.get(0), 0, replicas, replicas));
        }
        TopicMetadata t = new TopicMetadata(req.name(), parts, rf, minIsr);
        await(raft.propose(m -> m.topics().containsKey(req.name()) ? m : m.withTopic(t)));
        raft.note("토픽 " + req.name() + " 생성: 파티션 " + req.partitions() + "개 × 사본 " + rf + " (최소 ISR " + minIsr + ")");
        return info(t);
    }

    private static TopicInfo info(TopicMetadata t) {
        return new TopicInfo(t.name(), t.partitions().size(), t.replicationFactor(), t.minInsyncReplicas());
    }

    // ------------------------------------------------------------------ ISR 변경 (파티션 리더가 요청)

    public void alterIsr(ReplicaManager.AlterIsr req) {
        if (!raft.isLeader()) {
            forwardToController("/controller/alter-isr", req, Void.class);
            return;
        }
        PartitionMetadata pm = raft.accepted().partition(req.topic(), req.partition())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "없는 파티션"));
        if (pm.leader() != req.leaderId() || pm.leaderEpoch() != req.leaderEpoch()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "FENCED: 낡은 리더의 ISR 변경 요청");
        }
        if (!req.isr().contains(pm.leader()) || !pm.replicas().containsAll(req.isr())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ISR 은 리더를 포함한 사본의 부분집합");
        }
        List<Integer> before = pm.isr();
        if (new java.util.TreeSet<>(before).equals(new java.util.TreeSet<>(req.isr()))) return; // 이미 그렇다
        await(raft.propose(m -> m.partition(req.topic(), req.partition())
                .filter(cur -> cur.leaderEpoch() == req.leaderEpoch())
                .map(cur -> m.withPartition(req.topic(), cur.withIsr(req.isr())))
                .orElse(m)));
        raft.note(req.topic() + "-" + req.partition() + " ISR " + before + " → " + req.isr()
                + (req.isr().size() < before.size() ? " (뒤처진 사본 제외)" : " (따라잡은 사본 복귀)"));
    }

    // ------------------------------------------------------------------ 생사 감시와 장애 복구

    @Scheduled(fixedDelay = 500)
    void checkLiveness() {
        if (!raft.isLeader()) return;
        ClusterMetadata m = raft.accepted();
        if (!m.topics().containsKey(Topics.CONSUMER_OFFSETS) && m.controllerId() == props.brokerId()) {
            try {
                create(new TopicInfo(Topics.CONSUMER_OFFSETS, Topics.CONSUMER_OFFSETS_PARTITIONS, 3, 2));
            } catch (ResponseStatusException ignored) {
                // 다음 tick 에 다시
            }
            return;
        }
        Map<Integer, Long> ages = raft.peerAckAges();
        for (BrokerInfo b : m.brokers().values()) {
            boolean alive = b.id() == props.brokerId() || ages.getOrDefault(b.id(), Long.MAX_VALUE) <= props.brokerDeadMs();
            if (alive == b.alive()) continue;
            if (alive) {
                raft.note("broker-" + b.id() + " 응답 재개 → 살아남으로 표시");
                raft.propose(x -> onBrokerAlive(x.withBrokerAlive(b.id(), true), b.id()));
            } else {
                raft.note("broker-" + b.id() + " 가 " + props.brokerDeadMs() + "ms 동안 응답 없음 → 죽음으로 표시, 리더 다시 뽑기");
                raft.propose(x -> onBrokerDead(x.withBrokerAlive(b.id(), false), b.id()));
            }
        }
    }

    /** 그림 4.28: 죽은 브로커를 ISR 에서 빼고, 그 브로커가 리더였으면 ISR 에서 살아 있는 다음 사본을 리더로. */
    static ClusterMetadata onBrokerDead(ClusterMetadata m, int dead) {
        ClusterMetadata out = m;
        for (TopicMetadata t : m.topics().values()) {
            for (PartitionMetadata p : t.partitions()) {
                if (!p.isr().contains(dead) && p.leader() != dead) continue;
                List<Integer> isr = new ArrayList<>(p.isr());
                isr.remove(Integer.valueOf(dead));
                PartitionMetadata next;
                if (p.leader() != dead) {
                    next = p.withIsr(isr);
                } else {
                    Integer newLeader = p.replicas().stream()
                            .filter(isr::contains)
                            .filter(id -> m.brokers().get(id).alive())
                            .findFirst().orElse(null);
                    // ISR 이 모두 죽었다: 리더 없음. ISR 에는 마지막 리더를 남겨 그 브로커가 돌아오면 다시 맡긴다
                    next = newLeader == null ? p.withLeader(-1, List.of(dead)) : p.withLeader(newLeader, isr);
                }
                out = out.withPartition(t.name(), next);
            }
        }
        return out;
    }

    /** 리더 없는 파티션의 마지막 ISR 브로커가 돌아오면 다시 리더로 (깨끗한 리더 선출만 허용). */
    static ClusterMetadata onBrokerAlive(ClusterMetadata m, int back) {
        ClusterMetadata out = m;
        for (TopicMetadata t : m.topics().values()) {
            for (PartitionMetadata p : t.partitions()) {
                if (p.leader() == -1 && p.isr().contains(back)) out = out.withPartition(t.name(), p.withLeader(back, List.of(back)));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ 도우미

    private <T> T forwardToController(String path, Object req, Class<T> type) {
        String url = raft.leaderUrl();
        if (url == null) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "컨트롤러 선출 중");
        try {
            return new MiniMqHttp(url).post(path, req, type, Duration.ofSeconds(5));
        } catch (MiniMqHttp.HttpStatusException e) {
            throw new ResponseStatusException(HttpStatus.valueOf(e.status()), e.getMessage());
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "컨트롤러 broker-" + raft.leaderId() + " 에 전달 실패");
        }
    }

    private static void await(java.util.concurrent.CompletableFuture<?> f) {
        try {
            f.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "과반의 확인을 3초 안에 못 받음 (커밋 여부 미상)");
        } catch (ExecutionException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "중단됨");
        }
    }
}
