package study.minimq.common;

/** 파티션 하나가 어느 브로커에 있는지. 3단계에서 사본 목록·ISR 이 붙는다. */
public record PartitionMetadata(int partition, int leader) {
}
