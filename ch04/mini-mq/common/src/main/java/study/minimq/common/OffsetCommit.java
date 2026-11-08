package study.minimq.common;

/**
 * 소비자 그룹의 오프셋 커밋. 교재(그림 4.21)처럼 "여기까지 소비했다"는 마지막 오프셋을 적는다.
 * 아직 아무것도 소비하지 않았으면 -1.
 */
public record OffsetCommit(String group, String topic, int partition, long offset) {
}
