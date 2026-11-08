package study.minimq.common;

import java.util.ArrayList;
import java.util.List;

/** @param minInsyncReplicas ACK=all 쓰기는 ISR 이 이 수 이상일 때만 받는다 (교재 밖: min.insync.replicas) */
public record TopicMetadata(String name, List<PartitionMetadata> partitions, int replicationFactor, int minInsyncReplicas) {

    public TopicMetadata withPartition(PartitionMetadata p) {
        List<PartitionMetadata> ps = new ArrayList<>(partitions);
        ps.set(p.partition(), p);
        return new TopicMetadata(name, ps, replicationFactor, minInsyncReplicas);
    }
}
