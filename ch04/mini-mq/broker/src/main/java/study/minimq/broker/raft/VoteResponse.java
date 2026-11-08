package study.minimq.broker.raft;

public record VoteResponse(long term, boolean granted) {
}
