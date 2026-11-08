package study.minimq.broker;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.core.type.TypeReference;

import jakarta.annotation.PreDestroy;
import study.minimq.broker.raft.RaftNode;
import study.minimq.client.MiniMqHttp;
import study.minimq.common.ClusterMetadata;
import study.minimq.common.FetchResponse;
import study.minimq.common.PartitionMetadata;
import study.minimq.common.ProduceRecord;
import study.minimq.common.TopicInfo;
import study.minimq.common.TopicMetadata;
import study.minimq.common.Topics;

/**
 * 복제 (그림 4.23~4.27). 이 브로커가 사본을 가진 파티션마다 리더 또는 팔로워 역할을 한다.
 *
 * <h3>리더</h3>
 * 생산자 요청을 받아 로그 끝에 붙인다. 팔로워가 가져가러 올 때 알려 주는 오프셋으로 팔로워의 LEO 를 알고,
 * ISR 전원의 LEO 중 최솟값으로 HW(합의 오프셋)를 올린다. 너무 뒤처지거나 오래 안 오는 팔로워는 ISR 에서 빼 달라고
 * 컨트롤러에 요청한다(다시 따라잡으면 넣어 달라고).
 *
 * <h3>팔로워</h3>
 * 리더에게서 계속 가져간다(pull). 새 리더를 따르기 시작할 때 먼저 "내 마지막 epoch 가 리더 로그에서 어디까지인가"를
 * 물어 그 뒤로 갈라진 꼬리를 잘라낸다.
 */
@Component
public class ReplicaManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);

    /** 리더가 아는 팔로워의 상태. */
    record FollowerState(long logEnd, long lastFetchMs) {
    }

    final class Replica {
        final String topic;
        final int partition;
        final PartitionLog log;
        volatile PartitionMetadata meta;
        volatile int minIsr = 1;
        volatile int roleEpoch = -1;        // 이 epoch 로 역할을 시작했다
        volatile boolean leader;
        volatile Future<?> fetcher;
        volatile boolean isrChangeInFlight;
        volatile long leaderSinceMs;
        final Map<Integer, FollowerState> followers = new ConcurrentHashMap<>(); // 리더가 된 뒤 한 번이라도 온 팔로워만

        Replica(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
            this.log = new PartitionLog(topic, partition, dataDir.resolve(topic).resolve("Partition-" + partition),
                    props.segmentBytes(), props.flushEveryWrite());
        }

        String name() {
            return topic + "-" + partition;
        }
    }

    private final BrokerProperties props;
    private final RaftNode raft;
    private final Path dataDir;
    private final Map<String, Replica> replicas = new ConcurrentHashMap<>();
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, MiniMqHttp> clients = new ConcurrentHashMap<>();
    private final List<BiConsumer<String, Integer>> leaderListeners = new CopyOnWriteArrayList<>();
    private volatile ClusterMetadata meta;
    private volatile long replicationDelayMs; // 실험용: 팔로워가 가져가기 전에 쉬는 시간

    public ReplicaManager(BrokerProperties props, RaftNode raft) {
        this.props = props;
        this.raft = raft;
        this.dataDir = Path.of(props.dataDir());
        this.meta = raft.committed();
        raft.addListener(this::onMetadata);
    }

    /** 이 브로커가 어떤 파티션의 리더가 될 때 불린다 (오프셋 저장소가 자기 상태를 다시 읽는 데 쓴다). */
    public void onBecomeLeader(BiConsumer<String, Integer> listener) {
        leaderListeners.add(listener);
    }

    // ------------------------------------------------------------------ 역할 바꾸기

    private synchronized void onMetadata(ClusterMetadata m) {
        meta = m;
        int me = props.brokerId();
        for (TopicMetadata t : m.topics().values()) {
            for (PartitionMetadata pm : t.partitions()) {
                if (!pm.replicas().contains(me)) continue;
                Replica r = replicas.computeIfAbsent(t.name() + "-" + pm.partition(), k -> new Replica(t.name(), pm.partition()));
                r.meta = pm;
                r.minIsr = Math.max(1, t.minInsyncReplicas());
                if (pm.leaderEpoch() != r.roleEpoch) changeRole(r, pm);
                else if (r.leader) maybeAdvanceHighWatermark(r); // ISR 가 줄면 HW 가 오를 수 있다
            }
        }
    }

    private void changeRole(Replica r, PartitionMetadata pm) {
        Future<?> old = r.fetcher;
        if (old != null) old.cancel(true);
        r.fetcher = null;
        r.roleEpoch = pm.leaderEpoch();
        int me = props.brokerId();
        if (pm.leader() == me) {
            r.leader = true;
            r.log.assignEpoch(pm.leaderEpoch());
            // 팔로워 위치는 아직 모른다. 첫 요청이 올 때까지 HW 를 올리지 않고, ISR 에서도 바로 빼지 않는다
            r.followers.clear();
            r.leaderSinceMs = System.currentTimeMillis();
            raft.note(r.name() + " 의 리더가 됨 (leader epoch " + pm.leaderEpoch() + ", LEO " + r.log.logEndOffset() + ")");
            maybeAdvanceHighWatermark(r);
            leaderListeners.forEach(l -> l.accept(r.topic, r.partition));
        } else {
            r.leader = false;
            if (pm.leader() >= 0) {
                String url = meta.brokers().get(pm.leader()).url();
                int epoch = pm.leaderEpoch();
                r.fetcher = pool.submit(() -> followLoop(r, epoch, pm.leader(), url));
            }
        }
    }

    // ------------------------------------------------------------------ 팔로워

    private void followLoop(Replica r, int epoch, int leaderId, String url) {
        MiniMqHttp leader = http(url);
        boolean needTruncate = true;
        while (r.roleEpoch == epoch && !Thread.currentThread().isInterrupted()) {
            try {
                if (raft.isolated()) {
                    Thread.sleep(200);
                    continue;
                }
                if (needTruncate) {
                    truncateToLeader(r, leader, leaderId);
                    needTruncate = false;
                }
                if (replicationDelayMs > 0) Thread.sleep(replicationDelayMs);
                long offset = r.log.logEndOffset();
                FetchResponse res = leader.get("/replica/fetch?topic=%s&partition=%d&offset=%d&replicaId=%d&leaderEpoch=%d&max=2000&waitMs=500"
                        .formatted(r.topic, r.partition, offset, props.brokerId(), epoch),
                        new TypeReference<FetchResponse>() {}, Duration.ofSeconds(3));
                if (r.roleEpoch != epoch) return;
                r.log.appendReplica(res.messages());
                r.log.advanceHighWatermark(res.highWatermark());
            } catch (InterruptedException e) {
                return;
            } catch (MiniMqHttp.HttpStatusException e) {
                if (e.status() == 412) needTruncate = true; // 리더보다 앞서 있다 → 다시 맞춘다
                sleepQuietly(200);
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) return;
                sleepQuietly(300);
            }
        }
    }

    /** KIP-101: 내 마지막 epoch 가 리더 로그에서 끝나는 곳 뒤를 잘라낸다. */
    private void truncateToLeader(Replica r, MiniMqHttp leader, int leaderId) throws IOException {
        int myEpoch = r.log.latestEpoch();
        long myEnd = r.log.logEndOffset();
        if (myEpoch < 0 || myEnd == 0) return;
        long end = leader.get("/replica/epoch-end?topic=%s&partition=%d&epoch=%d".formatted(r.topic, r.partition, myEpoch),
                new TypeReference<Long>() {}, Duration.ofSeconds(2));
        if (end < myEnd) {
            r.log.truncateTo(end);
            raft.note("✂ " + r.name() + " 꼬리 " + (myEnd - end) + "건 잘라냄 (LEO " + myEnd + " → " + end
                    + ", 새 리더 broker-" + leaderId + " 에 없는 메시지)");
        }
    }

    // ------------------------------------------------------------------ 리더: 팔로워가 가져가기

    /** 팔로워의 요청. 요청한 offset = 팔로워의 LEO 이므로 이걸로 HW 를 올린다. 새 데이터가 없으면 waitMs 동안 기다린다. */
    public CompletableFuture<FetchResponse> replicaFetch(String topic, int partition, long offset, int replicaId,
                                                         int leaderEpoch, int max, long waitMs) {
        Replica r = leaderReplica(topic, partition);
        if (leaderEpoch != r.roleEpoch) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "FENCED: leader epoch " + leaderEpoch + " ≠ " + r.roleEpoch);
        }
        if (offset > r.log.logEndOffset()) {
            throw new ResponseStatusException(HttpStatus.PRECONDITION_FAILED, "팔로워가 리더보다 앞서 있다. 잘라내고 다시 오라");
        }
        r.followers.put(replicaId, new FollowerState(offset, System.currentTimeMillis()));
        maybeAdvanceHighWatermark(r);
        return r.log.awaitLogEnd(offset + 1)
                .completeOnTimeout(null, waitMs, TimeUnit.MILLISECONDS)
                .thenApply(x -> new FetchResponse(topic, partition, r.log.read(offset, max, Long.MAX_VALUE),
                        r.log.logStartOffset(), r.log.logEndOffset(), r.log.highWatermark()));
    }

    public long epochEnd(String topic, int partition, int epoch) {
        return leaderReplica(topic, partition).log.endOffsetForEpoch(epoch);
    }

    private void maybeAdvanceHighWatermark(Replica r) {
        if (!r.leader) return;
        long hw = r.log.logEndOffset();
        for (int id : r.meta.isr()) {
            if (id == props.brokerId()) continue;
            FollowerState f = r.followers.get(id);
            hw = Math.min(hw, f == null ? r.log.highWatermark() : f.logEnd()); // 모르는 팔로워가 있으면 HW 는 제자리
        }
        r.log.advanceHighWatermark(hw);
    }

    // ------------------------------------------------------------------ 리더: 생산

    /**
     * ACK=0/1: 리더가 저장하면 바로 응답. ACK=all(-1): ISR 전원이 받아 HW 가 넘어가면 응답 (그림 4.25~4.27).
     * ACK=all 인데 ISR 이 최소 ISR 보다 적으면 저장하지 않고 거부한다 → 영속성 대신 가용성을 포기.
     */
    public CompletableFuture<long[]> produce(String topic, int partition, int acks, List<ProduceRecord> records) {
        Replica r = leaderReplica(topic, partition);
        if (acks < 0 && r.meta.isr().size() < r.minIsr) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "NOT_ENOUGH_REPLICAS: ISR " + r.meta.isr() + " < 최소 ISR " + r.minIsr);
        }
        long[] range = r.log.appendAsLeader(records, r.roleEpoch);
        maybeAdvanceHighWatermark(r); // ISR 이 리더 혼자면 바로 오른다
        if (acks >= 0 || records.isEmpty()) return CompletableFuture.completedFuture(range);
        CompletableFuture<long[]> done = new CompletableFuture<>();
        r.log.awaitHighWatermark(range[1] + 1)
                .orTimeout(props.ackTimeoutMs(), TimeUnit.MILLISECONDS)
                .whenComplete((x, e) -> {
                    if (e == null) done.complete(range);
                    else done.completeExceptionally(new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT,
                            "ACK=all: " + props.ackTimeoutMs() + "ms 안에 ISR " + r.meta.isr() + " 전원이 받지 못함 (저장은 됐을 수 있음)"));
                });
        return done;
    }

    // ------------------------------------------------------------------ 리더: 소비

    /** 소비자는 HW(합의 오프셋) 앞까지만 받는다. 아직 ISR 전원에 없는 메시지는 리더가 바뀌면 사라질 수 있으니까. */
    public CompletableFuture<FetchResponse> consumerFetch(String topic, int partition, long offset, int max, long waitMs) {
        Replica r = leaderReplica(topic, partition);
        CompletableFuture<Void> ready = waitMs <= 0
                ? CompletableFuture.completedFuture(null)
                : r.log.awaitHighWatermark(offset + 1).completeOnTimeout(null, waitMs, TimeUnit.MILLISECONDS);
        return ready.thenApply(x -> {
            long hw = r.log.highWatermark();
            return new FetchResponse(topic, partition, r.log.read(offset, max, hw), r.log.logStartOffset(), r.log.logEndOffset(), hw);
        });
    }

    /** 내가 리더인 파티션의 로그를 직접 읽는다 (오프셋 저장소가 다시 읽을 때). */
    public List<study.minimq.common.Message> readAll(String topic, int partition) {
        Replica r = leaderReplica(topic, partition);
        return r.log.read(0, Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    Replica leaderReplica(String topic, int partition) {
        PartitionMetadata pm = meta.partition(topic, partition)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "없는 파티션: " + topic + "-" + partition));
        Replica r = replicas.get(topic + "-" + partition);
        if (pm.leader() != props.brokerId() || r == null || !r.leader) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "NOT_LEADER: " + topic + "-" + partition + " 의 리더는 broker-" + pm.leader());
        }
        return r;
    }

    public boolean isLeader(String topic, int partition) {
        Replica r = replicas.get(topic + "-" + partition);
        return r != null && r.leader;
    }

    // ------------------------------------------------------------------ 리더: ISR 관리

    /**
     * 0.5초마다 ISR 을 다시 계산한다.
     * 빠지는 조건: replicaLagTimeMs 동안 안 왔거나, 리더보다 replicaLagMaxMessages 넘게 뒤처짐.
     * 다시 들어오는 조건: 최근에 왔고, HW 까지 따라잡았고, 뒤처진 정도가 기준 안.
     * 바뀌면 컨트롤러에 요청하고, 커밋된 메타데이터로 돌아온 뒤에야 HW 계산에 반영된다.
     */
    @Scheduled(fixedDelay = 500)
    void evaluateIsr() {
        long now = System.currentTimeMillis();
        int me = props.brokerId();
        for (Replica r : replicas.values()) {
            if (!r.leader || r.isrChangeInFlight) continue;
            PartitionMetadata pm = r.meta;
            long leo = r.log.logEndOffset();
            Set<Integer> desired = new TreeSet<>();
            desired.add(me);
            for (int id : pm.replicas()) {
                if (id == me) continue;
                FollowerState f = r.followers.get(id);
                if (f == null) {
                    // 리더가 된 뒤 아직 안 온 팔로워: 유예 시간 동안은 원래 ISR 그대로 둔다
                    if (pm.isr().contains(id) && now - r.leaderSinceMs <= props.replicaLagTimeMs()) desired.add(id);
                    continue;
                }
                boolean recent = now - f.lastFetchMs() <= props.replicaLagTimeMs();
                boolean close = leo - f.logEnd() <= props.replicaLagMaxMessages();
                boolean inIsr = pm.isr().contains(id);
                if (inIsr ? recent && close : recent && close && f.logEnd() >= r.log.highWatermark()) desired.add(id);
            }
            if (desired.equals(new TreeSet<>(pm.isr()))) continue;
            requestIsrChange(r, pm, new ArrayList<>(desired));
        }
    }

    private void requestIsrChange(Replica r, PartitionMetadata pm, List<Integer> isr) {
        String controller = raft.leaderUrl();
        if (controller == null || raft.isolated()) return;
        r.isrChangeInFlight = true;
        pool.submit(() -> {
            try {
                http(controller).post("/controller/alter-isr",
                        new AlterIsr(r.topic, r.partition, props.brokerId(), pm.leaderEpoch(), isr), Void.class, Duration.ofSeconds(3));
            } catch (IOException e) {
                log.debug("ISR 변경 요청 실패 {}: {}", r.name(), e.getMessage());
            } finally {
                r.isrChangeInFlight = false;
            }
        });
    }

    public record AlterIsr(String topic, int partition, int leaderId, int leaderEpoch, List<Integer> isr) {
    }

    // ------------------------------------------------------------------ 운영

    @Scheduled(fixedDelay = 10_000)
    void enforceRetention() {
        replicas.values().forEach(r -> {
            if (Topics.internal(r.topic)) return; // 오프셋은 지우지 않는다
            int n = r.log.deleteExpired(props.retentionMs());
            if (n > 0) log.info("retention: {} 에서 세그먼트 {}개 삭제", r.name(), n);
        });
    }

    public void setReplicationDelayMs(long ms) {
        replicationDelayMs = ms;
        raft.note(ms > 0 ? "🐢 복제 지연 시작: 팔로워가 가져가기 전에 " + ms + "ms 쉰다" : "복제 지연 해제");
    }

    public long replicationDelayMs() {
        return replicationDelayMs;
    }

    public List<TopicInfo> topics() {
        return meta.topics().values().stream()
                .map(t -> new TopicInfo(t.name(), t.partitions().size(), t.replicationFactor(), t.minInsyncReplicas()))
                .sorted(Comparator.comparing(TopicInfo::name)).toList();
    }

    /** 대시보드용: 이 브로커가 가진 사본들. */
    public List<Map<String, Object>> states() {
        long now = System.currentTimeMillis();
        List<Map<String, Object>> out = new ArrayList<>();
        replicas.values().stream().sorted(Comparator.comparing(Replica::name)).forEach(r -> {
            PartitionState st = r.log.state();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("topic", r.topic);
            m.put("partition", r.partition);
            m.put("leader", r.leader);
            m.put("leaderEpoch", r.roleEpoch);
            m.put("logStartOffset", st.logStartOffset());
            m.put("logEndOffset", st.logEndOffset());
            m.put("highWatermark", st.highWatermark());
            m.put("segments", st.segments());
            if (r.leader) {
                Map<Integer, Object> f = new LinkedHashMap<>();
                r.followers.forEach((id, s) -> f.put(id, Map.of("logEnd", s.logEnd(), "lastFetchAgoMs", now - s.lastFetchMs())));
                m.put("followers", f);
            }
            out.add(m);
        });
        return out;
    }

    private MiniMqHttp http(String url) {
        return clients.computeIfAbsent(url, MiniMqHttp::new);
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    void close() {
        pool.shutdownNow();
        replicas.values().forEach(r -> r.log.close());
    }
}
