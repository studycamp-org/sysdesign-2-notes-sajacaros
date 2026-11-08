package study.minimq.common;

/** 키가 있으면 hash(key) % numPartitions. 같은 키는 항상 같은 파티션 → 키별 순서 보장. */
public final class Partitioner {
    private Partitioner() {
    }

    public static int forKey(String key, int partitions) {
        return Math.floorMod(key.hashCode(), partitions);
    }
}
