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

    public MiniMqHttp(String bootstrap) {
        this.base = bootstrap.endsWith("/") ? bootstrap.substring(0, bootstrap.length() - 1) : bootstrap;
    }

    public <T> T get(String path, TypeReference<T> type, Duration timeout) throws IOException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + path)).timeout(timeout).GET().build();
        return Json.MAPPER.readValue(send(req), type);
    }

    public <T> T post(String path, Object body, Class<T> type) throws IOException {
        String s = send(postRequest(path, body));
        return type == Void.class || s.isEmpty() ? null : Json.MAPPER.readValue(s, type);
    }

    /** 응답을 기다리지 않는 전송 (ACK=0). */
    public CompletableFuture<?> postAndForget(String path, Object body) throws IOException {
        return client.sendAsync(postRequest(path, body), HttpResponse.BodyHandlers.discarding());
    }

    private HttpRequest postRequest(String path, Object body) throws IOException {
        return HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.MAPPER.writeValueAsBytes(body)))
                .build();
    }

    private String send(HttpRequest req) throws IOException {
        try {
            HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() >= 400) throw new IOException(req.uri().getPath() + " → HTTP " + res.statusCode() + " " + res.body());
            return res.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
