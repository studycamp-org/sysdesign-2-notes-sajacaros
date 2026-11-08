package study.minimq.broker.zk;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.Json;

/** ZooKeeper-lite 와 이야기하는 얇은 HTTP 클라이언트. 데이터는 문자열로 주고받는다. */
public final class ZkClient {

    public record Node(String path, String data, long version, long czxid, long mzxid, Long ephemeralOwner) {
    }

    public record Session(long id, String name) {
    }

    /** ZooKeeper-lite 가 거절했다 (code = NODE_EXISTS, NO_NODE, BAD_VERSION, CHECK_FAILED, NO_SESSION). */
    public static final class ZkError extends IOException {
        public final String code;

        ZkError(String code, String message) {
            super(message);
            this.code = code;
        }
    }

    private final String base;
    private final String who;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build();

    public ZkClient(String base, String who) {
        this.base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        this.who = who;
    }

    public Session openSession() throws IOException {
        return Json.MAPPER.readValue(send("POST", "/session?name=" + enc(who), null, 2000), Session.class);
    }

    public void ping(long session) throws IOException {
        send("POST", "/session/ping?id=" + session, null, 1000);
    }

    public Node create(String path, String data, Long ephemeralSession) throws IOException {
        String q = "/node?path=" + enc(path) + "&who=" + enc(who) + (ephemeralSession != null ? "&session=" + ephemeralSession : "");
        return Json.MAPPER.readValue(send("PUT", q, data, 2000), Node.class);
    }

    public Node get(String path) throws IOException {
        return Json.MAPPER.readValue(send("GET", "/node?path=" + enc(path), null, 2000), Node.class);
    }

    public Node set(String path, String data, long expectedVersion, String checkPath, Long checkCzxid) throws IOException {
        String q = "/node?path=" + enc(path) + "&who=" + enc(who) + "&expectedVersion=" + expectedVersion
                + (checkPath != null ? "&checkPath=" + enc(checkPath) + "&checkCzxid=" + checkCzxid : "");
        return Json.MAPPER.readValue(send("POST", q, data, 2000), Node.class);
    }

    public List<String> children(String path) throws IOException {
        return Json.MAPPER.readValue(send("GET", "/children?path=" + enc(path), null, 2000), new TypeReference<List<String>>() {});
    }

    /** 감시: 무엇이든 바뀌면(zxid 가 since 보다 커지면) 돌아온다. 최대 waitMs. */
    public long waitForChange(long since, long waitMs) throws IOException {
        return Long.parseLong(send("GET", "/wait?since=" + since + "&waitMs=" + waitMs, null, waitMs + 2000).trim());
    }

    public Map<String, Object> state() throws IOException {
        return Json.MAPPER.readValue(send("GET", "/admin/state", null, 700), new TypeReference<Map<String, Object>>() {});
    }

    private String send(String method, String path, String body, long timeoutMs) throws IOException {
        HttpRequest.BodyPublisher pub = body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body);
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "text/plain; charset=utf-8").method(method, pub).build();
        try {
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() == 409) {
                Map<String, String> err = Json.MAPPER.readValue(res.body(), new TypeReference<Map<String, String>>() {});
                throw new ZkError(err.getOrDefault("code", "?"), err.getOrDefault("message", res.body()));
            }
            if (res.statusCode() >= 400) throw new IOException("zookeeper-lite " + path + " → HTTP " + res.statusCode());
            return res.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
