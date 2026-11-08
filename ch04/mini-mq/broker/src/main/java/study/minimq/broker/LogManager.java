package study.minimq.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
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
import study.minimq.common.Json;
import study.minimq.common.TopicInfo;

/**
 * 토픽과 파티션 로그를 들고 있는 데이터 저장소.
 * 디렉터리 구조: {dataDir}/{topic}/Partition-{id}/segment-N.log (그림 4.10)
 *
 * <p>1단계에서는 토픽 목록(메타데이터)도 브로커 로컬 파일 topics.json 에 둔다.
 * 2단계에서 컨트롤러(Raft-lite)가 관리하는 메타데이터로 옮긴다.
 */
@Component
public class LogManager {
    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    private final BrokerProperties props;
    private final Path dataDir;
    private final Path topicsFile;
    private final Map<String, TopicInfo> topics = new ConcurrentHashMap<>();
    private final Map<String, PartitionLog> logs = new ConcurrentHashMap<>();

    public LogManager(BrokerProperties props) throws IOException {
        this.props = props;
        this.dataDir = Path.of(props.dataDir());
        this.topicsFile = dataDir.resolve("topics.json");
        Files.createDirectories(dataDir);
        if (Files.exists(topicsFile)) {
            for (TopicInfo t : Json.MAPPER.readValue(topicsFile.toFile(), TopicInfo[].class)) open(t);
        }
        log.info("broker {} data={} topics={}", props.brokerId(), dataDir.toAbsolutePath(), topics.keySet());
    }

    public synchronized TopicInfo createTopic(TopicInfo req) {
        if (req.name() == null || !req.name().matches("[A-Za-z0-9._-]+")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "토픽 이름은 영문·숫자·._- 만");
        }
        TopicInfo existing = topics.get(req.name());
        if (existing != null) return existing;
        if (req.partitions() < 1) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "파티션은 1개 이상");
        open(req);
        persistTopics();
        log.info("topic created {} partitions={}", req.name(), req.partitions());
        return req;
    }

    private void open(TopicInfo t) {
        topics.put(t.name(), t);
        for (int p = 0; p < t.partitions(); p++) {
            Path dir = dataDir.resolve(t.name()).resolve("Partition-" + p);
            logs.put(key(t.name(), p), new PartitionLog(t.name(), p, dir, props.segmentBytes(), props.flushEveryWrite()));
        }
    }

    private void persistTopics() {
        try {
            Path tmp = dataDir.resolve("topics.json.tmp");
            Json.MAPPER.writeValue(tmp.toFile(), topics.values());
            Files.move(tmp, topicsFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public PartitionLog log(String topic, int partition) {
        PartitionLog l = logs.get(key(topic, partition));
        if (l == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "없는 파티션: " + topic + "-" + partition);
        return l;
    }

    public Collection<TopicInfo> topics() {
        return topics.values().stream().sorted(Comparator.comparing(TopicInfo::name)).toList();
    }

    public List<PartitionState> states() {
        List<PartitionState> out = new ArrayList<>();
        for (TopicInfo t : topics()) {
            for (int p = 0; p < t.partitions(); p++) out.add(log(t.name(), p).state());
        }
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
