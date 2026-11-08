package study.minimq.broker.raft;

public record AppendResponse(long term, boolean success, long acceptedVersion) {
}
