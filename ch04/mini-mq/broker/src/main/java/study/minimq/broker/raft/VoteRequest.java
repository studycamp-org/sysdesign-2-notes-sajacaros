package study.minimq.broker.raft;

/** 후보가 표를 달라고 보낸다. lastMeta* 는 후보가 가진 메타데이터가 얼마나 최신인지. */
public record VoteRequest(long term, int candidateId, long lastMetaTerm, long lastMetaVersion) {
}
