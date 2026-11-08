package study.minimq.common;

/** 생산자가 보내는 한 건. 오프셋·파티션은 브로커가 붙인다. */
public record ProduceRecord(String key, String value, long timestamp) {
}
