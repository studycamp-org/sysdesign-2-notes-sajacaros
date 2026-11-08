package study.minimq.broker.raft;

import study.minimq.common.ClusterMetadata;

/**
 * 리더(컨트롤러)의 박동. 메타데이터 스냅샷 전체를 보낸다 (작아서 로그 대신 통째로. Raft-lite 의 단순화).
 *
 * @param metadata  리더가 받은 최신 스냅샷 (아직 커밋 전일 수 있다)
 * @param committed 리더가 커밋한 스냅샷. 함께 보내야 팔로워가 커밋을 따라올 수 있다.
 *                  스냅샷은 최신 것 하나만 들고 있어서, 변경이 계속 들어오면 "커밋된 버전"을 자기 손에 쥘 틈이 없기 때문
 */
public record AppendRequest(long term, int leaderId, ClusterMetadata metadata, long commitVersion, ClusterMetadata committed) {
}
