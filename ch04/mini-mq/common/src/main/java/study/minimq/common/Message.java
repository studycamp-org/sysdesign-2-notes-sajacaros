package study.minimq.common;

import java.nio.charset.StandardCharsets;

/**
 * 메시지 자료 구조 (교재 표 4.1). 생산자 → 큐 → 소비자로 가는 동안 모양이 바뀌지 않는 "계약".
 * topic + partition + offset 으로 메시지 하나를 특정한다.
 */
public record Message(
        String topic,
        int partition,
        long offset,
        String key,
        String value,
        long timestamp,
        int size,
        int crc) {

    public static Message create(String topic, int partition, long offset, ProduceRecord r) {
        int size = bytes(r.key()) + bytes(r.value());
        return new Message(topic, partition, offset, r.key(), r.value(), r.timestamp(), size, Crc.of(r.key(), r.value()));
    }

    /** 순환 중복 검사로 무결성 확인. */
    public boolean crcValid() {
        return crc == Crc.of(key, value);
    }

    private static int bytes(String s) {
        return s == null ? 0 : s.getBytes(StandardCharsets.UTF_8).length;
    }
}
