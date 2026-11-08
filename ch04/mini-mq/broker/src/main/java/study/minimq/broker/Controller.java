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

/**
 * 컨트롤러가 하는 일 (정리.md 「컨트롤러 정리」). Raft 리더인 브로커에서만 실제로 동작한다.
 * <ul>
 *   <li>파티션 배정: 토픽을 만들면 파티션을 살아 있는 브로커에 고르게 나눈다</li>
 *   <li>서비스 탐색: 박동 응답이 끊긴 브로커를 죽음으로 표시한다</li>
 * </ul>
 * 2단계는 사본이 하나뿐이라, 죽은 브로커의 파티션은 옮길 데이터가 없어 그대로 멈춘다. 3단계에서 사본으로 넘긴다.
 */
@Component
public class Controller {
    private final RaftNode raft;
    private final BrokerProperties props;

    public Controller(RaftNode raft, BrokerProperties props) {
        this.raft = raft;
        this.props = props;
    }

    public TopicInfo createTopic(TopicInfo req) {
        if (req.name() == null || !req.name().matches("[A-Za-z0-9._-]+")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "토픽 이름은 영문·숫자·._- 만");
        }
        if (req.partitions() < 1) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "파티션은 1개 이상");
        if (!raft.isLeader()) return forwardToController(req);

        TopicMetadata existing = raft.accepted().topics().get(req.name());
        if (existing != null) return new TopicInfo(existing.name(), existing.partitions().size());

        List<Integer> alive = raft.accepted().brokers().values().stream()
                .filter(BrokerInfo::alive).map(BrokerInfo::id).sorted().toList();
        List<PartitionMetadata> parts = new ArrayList<>();
        for (int p = 0; p < req.partitions(); p++) parts.add(new PartitionMetadata(p, alive.get(p % alive.size())));
        try {
            raft.propose(m -> m.topics().containsKey(req.name()) ? m : m.withTopic(new TopicMetadata(req.name(), parts)))
                    .get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "과반의 확인을 3초 안에 못 받음 (커밋 여부 미상)");
        } catch (ExecutionException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "중단됨");
        }
        raft.note("토픽 " + req.name() + " 생성: 파티션 " + req.partitions() + "개를 broker " + alive + " 에 배정");
        return req;
    }

    private TopicInfo forwardToController(TopicInfo req) {
        String url = raft.leaderUrl();
        if (url == null) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "컨트롤러 선출 중");
        try {
            return new MiniMqHttp(url).post("/topics", req, TopicInfo.class, Duration.ofSeconds(5));
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "컨트롤러 broker-" + raft.leaderId() + " 에 전달 실패");
        }
    }

    /** 박동 응답이 brokerDeadMs 동안 없으면 죽음, 다시 오면 살아남으로 메타데이터를 바꾼다. */
    @Scheduled(fixedDelay = 500)
    void checkLiveness() {
        if (!raft.isLeader()) return;
        Map<Integer, Long> ages = raft.peerAckAges();
        ClusterMetadata m = raft.accepted();
        for (BrokerInfo b : m.brokers().values()) {
            boolean alive = b.id() == props.brokerId() || ages.getOrDefault(b.id(), Long.MAX_VALUE) <= props.brokerDeadMs();
            if (alive != b.alive()) {
                raft.note(alive ? "broker-" + b.id() + " 응답 재개 → 살아남으로 표시"
                        : "broker-" + b.id() + " 가 " + props.brokerDeadMs() + "ms 동안 응답 없음 → 죽음으로 표시");
                raft.propose(x -> x.withBrokerAlive(b.id(), alive));
            }
        }
    }
}
