package study.minimq.client;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.ClusterMetadata;
import study.minimq.common.TopicMetadata;

/**
 * 클라이언트가 들고 있는 "지도" (정리.md: 지도를 그리는 건 컨트롤러, 보고 찾아가는 건 프로듀서).
 * 부트스트랩 브로커 아무에게나 메타데이터를 받아 캐시하고, 파티션 리더의 주소를 알려 준다.
 * 리더가 아니라는 답(409)이나 연결 실패를 받으면 {@link #invalidate()} 로 지도를 버리고 다시 받는다.
 */
public final class ClusterView {
    private final List<String> bootstraps;
    private final Map<String, MiniMqHttp> clients = new ConcurrentHashMap<>();
    private volatile ClusterMetadata meta;

    public ClusterView(String bootstrapCsv) {
        this.bootstraps = Arrays.stream(bootstrapCsv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    public synchronized ClusterMetadata refresh() throws IOException {
        IOException last = new IOException("부트스트랩 브로커가 없다");
        for (String b : bootstraps) {
            try {
                meta = http(b).get("/metadata", new TypeReference<ClusterMetadata>() {}, Duration.ofSeconds(2));
                return meta;
            } catch (IOException e) {
                last = e;
            }
        }
        throw last;
    }

    public ClusterMetadata metadata() throws IOException {
        ClusterMetadata m = meta;
        return m != null ? m : refresh();
    }

    public void invalidate() {
        meta = null;
    }

    /** 방금 만든 토픽은 팔로워 브로커의 메타데이터에 박동 한 번(≤0.5초) 늦게 나타나므로 잠깐 기다린다. */
    public int partitions(String topic) throws IOException {
        TopicMetadata t = metadata().topics().get(topic);
        for (int i = 0; t == null && i < 10; i++) {
            if (i > 0) sleep(200);
            t = refresh().topics().get(topic);
        }
        if (t == null) throw new IOException("없는 토픽: " + topic);
        return t.partitions().size();
    }

    private static void sleep(long ms) throws IOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    /** 파티션 리더 브로커에 붙은 클라이언트. */
    public MiniMqHttp leader(String topic, int partition) throws IOException {
        String url = metadata().leaderUrl(topic, partition);
        if (url == null) throw new IOException("리더를 모름: " + topic + "-" + partition);
        return http(url);
    }

    public int leaderId(String topic, int partition) throws IOException {
        return metadata().partition(topic, partition).map(p -> p.leader()).orElse(-1);
    }

    /** 부트스트랩 중 응답하는 아무 브로커. */
    public MiniMqHttp any() {
        return http(bootstraps.get((int) (System.nanoTime() % bootstraps.size())));
    }

    public List<String> bootstraps() {
        return bootstraps;
    }

    public MiniMqHttp http(String url) {
        return clients.computeIfAbsent(url, MiniMqHttp::new);
    }
}
