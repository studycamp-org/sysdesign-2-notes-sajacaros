package study.minimq.broker;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import jakarta.annotation.PreDestroy;
import study.minimq.broker.raft.RaftNode;
import study.minimq.common.ClusterMetadata;
import study.minimq.common.PartitionMetadata;
import study.minimq.common.TopicInfo;
import study.minimq.common.TopicMetadata;

/**
 * 데이터 저장소. 디렉터리 구조: {dataDir}/{topic}/Partition-{id}/segment-N.log (그림 4.10)
 *
 * <p>어떤 파티션을 맡을지는 컨트롤러가 정한 메타데이터(커밋된 것)를 따른다.
 * 내가 리더가 아닌 파티션으로 요청이 오면 409 로 거절하고, 클라이언트는 메타데이터를 다시 받아 리더를 찾아간다.
 */
@Component
public class LogManager {
    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    private final BrokerProperties props;
    private final Path dataDir;
    private final Map<String, PartitionLog> logs = new ConcurrentHashMap<>();
    private volatile ClusterMetadata meta;

    public LogManager(BrokerProperties props, RaftNode raft) {
        this.props = props;
        this.dataDir = Path.of(props.dataDir());
        this.meta = raft.committed();
        raft.addListener(this::onMetadata);
    }

    private void onMetadata(ClusterMetadata m) {
        meta = m;
        for (TopicMetadata t : m.topics().values()) {
            for (PartitionMetadata p : t.partitions()) {
                if (p.leader() == props.brokerId()) open(t.name(), p.partition());
            }
        }
    }

    private PartitionLog open(String topic, int partition) {
        return logs.computeIfAbsent(key(topic, partition), k -> {
            log.info("파티션 {} 을 맡는다", k);
            return new PartitionLog(topic, partition, dataDir.resolve(topic).resolve("Partition-" + partition),
                    props.segmentBytes(), props.flushEveryWrite());
        });
    }

    /** 이 브로커가 리더인 파티션의 로그. 아니면 404(없는 파티션) 또는 409(리더가 다른 브로커). */
    public PartitionLog log(String topic, int partition) {
        PartitionMetadata pm = meta.partition(topic, partition)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "없는 파티션: " + topic + "-" + partition));
        if (pm.leader() != props.brokerId()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "NOT_LEADER: " + topic + "-" + partition + " 의 리더는 broker-" + pm.leader());
        }
        return open(topic, partition);
    }

    public boolean isLeader(String topic, int partition) {
        return meta.partition(topic, partition).map(p -> p.leader() == props.brokerId()).orElse(false);
    }

    public List<TopicInfo> topics() {
        return meta.topics().values().stream()
                .map(t -> new TopicInfo(t.name(), t.partitions().size()))
                .sorted(Comparator.comparing(TopicInfo::name)).toList();
    }

    /** 이 브로커가 들고 있는 파티션들의 상태. */
    public List<PartitionState> states() {
        List<PartitionState> out = new ArrayList<>();
        logs.values().forEach(l -> out.add(l.state()));
        out.sort(Comparator.comparing(PartitionState::topic).thenComparing(PartitionState::partition));
        return out;
    }

    @Scheduled(fixedDelay = 10_000)
    void enforceRetention() {
        logs.forEach((k, l) -> {
            int n = l.deleteExpired(props.retentionMs());
            if (n > 0) log.info("retention: {} 에서 세그먼트 {}개 삭제", k, n);
        });
    }

    @PreDestroy
    void close() {
        logs.values().forEach(PartitionLog::close);
    }

    private static String key(String topic, int partition) {
        return topic + "-" + partition;
    }
}
