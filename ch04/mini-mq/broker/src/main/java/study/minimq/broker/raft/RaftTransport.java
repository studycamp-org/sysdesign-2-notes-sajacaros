package study.minimq.broker.raft;

import java.io.IOException;

/** 다른 노드에 RPC 를 보낸다. 실제로는 HTTP, 테스트에서는 메모리 안에서 직접 부른다. */
public interface RaftTransport {
    VoteResponse vote(String url, VoteRequest req) throws IOException;

    AppendResponse append(String url, AppendRequest req) throws IOException;
}
