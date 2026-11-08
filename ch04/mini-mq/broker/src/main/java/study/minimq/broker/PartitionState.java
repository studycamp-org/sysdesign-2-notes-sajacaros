package study.minimq.broker;

import java.util.List;

/** 대시보드용 파티션 사본 상태. */
public record PartitionState(String topic, int partition, long logStartOffset, long logEndOffset, long highWatermark,
                             List<SegmentInfo> segments) {
    public record SegmentInfo(String file, long baseOffset, int count, long bytes, boolean active) {
    }
}
