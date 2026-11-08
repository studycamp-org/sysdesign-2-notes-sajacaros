package study.minimq.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import study.minimq.common.Message;
import study.minimq.common.OffsetCommit;
import study.minimq.common.ProduceRecord;
import study.minimq.common.Topics;

/**
 * 상태 저장소: 소비자 그룹별·파티션별 마지막으로 소비한 오프셋 (그림 4.21).
 *
 * <p>카프카처럼 내부 토픽 {@code __consumer_offsets} 에 "그룹|토픽|파티션 → 오프셋" 레코드로 쓴다.
 * 그룹 이름 해시로 파티션이 정해지고(그림 4.15), 그 파티션의 리더 브로커가 이 그룹의 오프셋을 맡는다.
 * 일반 메시지와 똑같이 복제되므로 리더가 죽어도 새 리더가 로그를 다시 읽어 이어 간다.
 */
@Component
public class OffsetStore {
    private final ReplicaManager replicas;
    private final Map<Integer, Map<String, Long>> cache = new ConcurrentHashMap<>(); // 오프셋 파티션 → (키 → 오프셋)

    public OffsetStore(ReplicaManager replicas) {
        this.replicas = replicas;
        replicas.onBecomeLeader((topic, partition) -> {
            if (topic.equals(Topics.CONSUMER_OFFSETS)) rebuild(partition);
        });
    }

    /** 리더가 되면 로그를 처음부터 읽어 최신 값을 다시 만든다. */
    private void rebuild(int partition) {
        Map<String, Long> m = new ConcurrentHashMap<>();
        for (Message msg : replicas.readAll(Topics.CONSUMER_OFFSETS, partition)) m.put(msg.key(), Long.parseLong(msg.value()));
        cache.put(partition, m);
    }

    /** 한 그룹의 여러 파티션 오프셋을 한 번에, ACK=all 로 쓴다. */
    public CompletableFuture<Void> commit(List<OffsetCommit> commits) {
        if (commits.isEmpty()) return CompletableFuture.completedFuture(null);
        String group = commits.get(0).group();
        if (commits.stream().anyMatch(c -> !c.group().equals(group))) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "한 번에 한 그룹만");
        }
        int p = Topics.offsetsPartition(group);
        List<ProduceRecord> records = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (OffsetCommit c : commits) records.add(new ProduceRecord(key(c.group(), c.topic(), c.partition()), Long.toString(c.offset()), now));
        return replicas.produce(Topics.CONSUMER_OFFSETS, p, -1, records).thenAccept(range -> {
            Map<String, Long> m = cache.computeIfAbsent(p, x -> new ConcurrentHashMap<>());
            records.forEach(r -> m.put(r.key(), Long.parseLong(r.value())));
        });
    }

    /** 그룹이 이 토픽에서 파티션별로 마지막으로 소비한 오프셋. 없는 파티션은 빠진다. */
    public Map<Integer, Long> committed(String group, String topic) {
        int p = Topics.offsetsPartition(group);
        replicas.leaderReplica(Topics.CONSUMER_OFFSETS, p); // 이 브로커가 코디네이터가 아니면 409
        Map<Integer, Long> out = new TreeMap<>();
        String prefix = group + "|" + topic + "|";
        cache.getOrDefault(p, Map.of()).forEach((k, v) -> {
            if (k.startsWith(prefix)) out.put(Integer.parseInt(k.substring(prefix.length())), v);
        });
        return out;
    }

    /** 대시보드용: 이 브로커가 코디네이터인 그룹들의 오프셋. */
    public List<OffsetCommit> all() {
        List<OffsetCommit> out = new ArrayList<>();
        cache.forEach((p, m) -> {
            if (!replicas.isLeader(Topics.CONSUMER_OFFSETS, p)) return;
            m.forEach((k, v) -> {
                String[] parts = k.split("\\|");
                out.add(new OffsetCommit(parts[0], parts[1], Integer.parseInt(parts[2]), v));
            });
        });
        return out;
    }

    private static String key(String group, String topic, int partition) {
        return group + "|" + topic + "|" + partition;
    }
}
