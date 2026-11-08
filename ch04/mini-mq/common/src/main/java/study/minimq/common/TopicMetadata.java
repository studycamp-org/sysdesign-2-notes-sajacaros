package study.minimq.common;

import java.util.List;

public record TopicMetadata(String name, List<PartitionMetadata> partitions) {
}
