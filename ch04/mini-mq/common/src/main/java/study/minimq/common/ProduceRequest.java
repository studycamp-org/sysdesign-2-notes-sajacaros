package study.minimq.common;

import java.util.List;

/** 한 파티션으로 가는 묶음(배치). acks 는 2단계(복제)부터 의미가 생긴다. */
public record ProduceRequest(String topic, int partition, int acks, List<ProduceRecord> records) {
}
