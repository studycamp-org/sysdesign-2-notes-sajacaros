package study.minimq.broker.raft;

import study.minimq.common.ClusterMetadata;

/**
 * 리더(컨트롤러)의 박동. 메타데이터 스냅샷 전체와 "여기까지 커밋됐다"를 함께 보낸다.
 * 메타데이터가 작아서 로그 대신 통째로 보낸다 (Raft-lite 의 단순화).
 */
public record AppendRequest(long term, int leaderId, ClusterMetadata metadata, long commitVersion) {
}
