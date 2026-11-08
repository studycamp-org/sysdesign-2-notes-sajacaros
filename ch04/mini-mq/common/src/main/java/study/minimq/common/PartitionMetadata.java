package study.minimq.common;

import java.util.List;

/**
 * 파티션 하나의 사본 분산 계획 (그림 4.23).
 *
 * @param leader      리더 사본이 있는 브로커. -1 이면 ISR 이 모두 죽어 리더가 없음(오프라인)
 * @param leaderEpoch 리더가 바뀔 때마다 1씩 오른다. 낡은 리더·팔로워의 요청을 걸러 낸다
 * @param replicas    사본을 가진 브로커들 (첫 번째가 처음 리더)
 * @param isr         리더와 동기화된 사본 (리더 포함). 새 리더는 여기서만 뽑는다
 */
public record PartitionMetadata(int partition, int leader, int leaderEpoch, List<Integer> replicas, List<Integer> isr) {

    public PartitionMetadata withLeader(int newLeader, List<Integer> newIsr) {
        return new PartitionMetadata(partition, newLeader, leaderEpoch + 1, replicas, newIsr);
    }

    public PartitionMetadata withIsr(List<Integer> newIsr) {
        return new PartitionMetadata(partition, leader, leaderEpoch, replicas, newIsr);
    }
}
