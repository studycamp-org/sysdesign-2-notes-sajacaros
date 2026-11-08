package study.minimq.common;

/** 내부 토픽. 소비자 그룹 오프셋을 복제된 로그로 저장한다 (카프카와 같은 방식). */
public final class Topics {
    public static final String CONSUMER_OFFSETS = "__consumer_offsets";
    public static final int CONSUMER_OFFSETS_PARTITIONS = 3;

    private Topics() {
    }

    public static boolean internal(String topic) {
        return topic.startsWith("__");
    }

    /** 그룹의 오프셋이 들어갈 파티션. 이 파티션의 리더가 그 그룹의 코디네이터다 (그림 4.15: 그룹 이름 해시). */
    public static int offsetsPartition(String group) {
        return Partitioner.forKey(group, CONSUMER_OFFSETS_PARTITIONS);
    }
}
