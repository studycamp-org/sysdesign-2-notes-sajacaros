package study.minimq.broker.raft;

import study.minimq.common.ClusterMetadata;

/**
 * Raft 가 디스크에 남겨야 하는 것. 재시작해도 같은 term 에 두 번 투표하지 않으려면
 * currentTerm·votedFor 를 응답하기 전에 저장해야 한다.
 */
public interface RaftStorage {
    record State(long currentTerm, Integer votedFor, ClusterMetadata accepted, ClusterMetadata committed) {
    }

    State load();

    void save(State state);
}
