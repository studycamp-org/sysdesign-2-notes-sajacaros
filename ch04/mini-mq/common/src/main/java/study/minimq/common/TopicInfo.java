package study.minimq.common;

/** 토픽 생성 요청. replicationFactor·minInsyncReplicas 를 비우면(0) 3 / 1. */
public record TopicInfo(String name, int partitions, int replicationFactor, int minInsyncReplicas) {
    public TopicInfo(String name, int partitions) {
        this(name, partitions, 0, 0);
    }
}
