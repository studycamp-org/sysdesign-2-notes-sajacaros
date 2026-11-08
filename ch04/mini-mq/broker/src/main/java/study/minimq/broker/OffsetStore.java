package study.minimq.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.Json;
import study.minimq.common.OffsetCommit;

/**
 * 상태 저장소: 소비자 그룹별·파티션별 마지막으로 소비한 오프셋 (그림 4.21).
 *
 * <p>1단계 임시 구현: 브로커 로컬 파일 offsets.json. 4단계에서 카프카처럼
 * 내부 토픽 __consumer_offsets 로 옮겨 복제를 그대로 재사용한다.
 */
@Component
public class OffsetStore {
    private final Path file;
    private final Map<String, Long> offsets;

    public OffsetStore(BrokerProperties props) throws IOException {
        this.file = Path.of(props.dataDir()).resolve("offsets.json");
        this.offsets = Files.exists(file)
                ? new TreeMap<>(Json.MAPPER.readValue(file.toFile(), new TypeReference<Map<String, Long>>() {}))
                : new TreeMap<>();
    }

    public synchronized void commit(OffsetCommit c) {
        offsets.put(key(c.group(), c.topic(), c.partition()), c.offset());
        try {
            Path tmp = file.resolveSibling("offsets.json.tmp");
            Json.MAPPER.writeValue(tmp.toFile(), offsets);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized long committed(String group, String topic, int partition) {
        return offsets.getOrDefault(key(group, topic, partition), -1L);
    }

    public synchronized List<OffsetCommit> all() {
        List<OffsetCommit> out = new ArrayList<>();
        offsets.forEach((k, v) -> {
            String[] parts = k.split("\\|");
            out.add(new OffsetCommit(parts[0], parts[1], Integer.parseInt(parts[2]), v));
        });
        return out;
    }

    private static String key(String group, String topic, int partition) {
        return group + "|" + topic + "|" + partition;
    }
}
