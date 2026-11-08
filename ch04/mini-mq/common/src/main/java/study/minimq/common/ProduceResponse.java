package study.minimq.common;

/** 묶음의 첫 오프셋과 마지막 오프셋. */
public record ProduceResponse(String topic, int partition, long baseOffset, long lastOffset) {
}
