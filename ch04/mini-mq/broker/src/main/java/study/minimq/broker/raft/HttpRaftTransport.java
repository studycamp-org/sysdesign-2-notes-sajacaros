package study.minimq.broker.raft;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import study.minimq.client.MiniMqHttp;

/** Raft RPC 를 HTTP 로. 짧은 타임아웃: 죽은 노드를 오래 기다리면 박동이 밀린다. */
public final class HttpRaftTransport implements RaftTransport {
    private static final Duration TIMEOUT = Duration.ofMillis(400);
    private final Map<String, MiniMqHttp> clients = new ConcurrentHashMap<>();

    @Override
    public VoteResponse vote(String url, VoteRequest req) throws IOException {
        return http(url).post("/raft/vote", req, VoteResponse.class, TIMEOUT);
    }

    @Override
    public AppendResponse append(String url, AppendRequest req) throws IOException {
        return http(url).post("/raft/append", req, AppendResponse.class, TIMEOUT);
    }

    private MiniMqHttp http(String url) {
        return clients.computeIfAbsent(url, MiniMqHttp::new);
    }
}
