package study.minimq.client;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.Json;

/** 브로커와 HTTP/JSON 으로 이야기하는 얇은 래퍼. */
public final class MiniMqHttp {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String base;

    /** 브로커가 4xx·5xx 로 답했다. 409 = 이 브로커는 그 파티션의 리더가 아님, 503 = 지금은 못 받음. */
    public static final class HttpStatusException extends IOException {
        private final int status;

        public HttpStatusException(int status, String message) {
            super(message);
            this.status = status;
        }

        public int status() {
            return status;
        }
    }

    /** 브로커 대시보드가 "누가 보냈는지" 그릴 수 있게 모든 요청에 붙는 이름. 예) producer-logs, consumer-search */
    private static volatile String clientId;

    public static void setClientId(String id) {
        clientId = id;
    }

    private static HttpRequest.Builder tag(HttpRequest.Builder b) {
        String id = clientId;
        return id == null ? b : b.header("X-MiniMq-Client", id);
    }

    public String base() {
        return base;
    }

    public MiniMqHttp(String bootstrap) {
        this.base = bootstrap.endsWith("/") ? bootstrap.substring(0, bootstrap.length() - 1) : bootstrap;
    }

    public <T> T get(String path, TypeReference<T> type, Duration timeout) throws IOException {
        HttpRequest req = tag(HttpRequest.newBuilder(URI.create(base + path))).timeout(timeout).GET().build();
        return Json.MAPPER.readValue(send(req), type);
    }

    public <T> T post(String path, Object body, Class<T> type) throws IOException {
        return post(path, body, type, Duration.ofSeconds(10));
    }

    public <T> T post(String path, Object body, Class<T> type, Duration timeout) throws IOException {
        String s = send(postRequest(path, body, timeout));
        return type == Void.class || s.isEmpty() ? null : Json.MAPPER.readValue(s, type);
    }

    /** 응답을 기다리지 않는 전송 (ACK=0). */
    public CompletableFuture<?> postAndForget(String path, Object body) throws IOException {
        return client.sendAsync(postRequest(path, body, Duration.ofSeconds(10)), HttpResponse.BodyHandlers.discarding());
    }

    private HttpRequest postRequest(String path, Object body, Duration timeout) throws IOException {
        return tag(HttpRequest.newBuilder(URI.create(base + path)))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.MAPPER.writeValueAsBytes(body)))
                .build();
    }

    private String send(HttpRequest req) throws IOException {
        try {
            HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() >= 400) throw new HttpStatusException(res.statusCode(), req.uri().getPath() + " → HTTP " + res.statusCode() + " " + res.body());
            return res.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
