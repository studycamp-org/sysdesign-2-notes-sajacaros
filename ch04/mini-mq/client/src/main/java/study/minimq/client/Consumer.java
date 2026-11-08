package study.minimq.client;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.FetchResponse;
import study.minimq.common.Message;
import study.minimq.common.OffsetCommit;
import study.minimq.common.TopicInfo;

/**
 * 소비자 클라이언트 (그림 4.14, 4.15). 풀 모델 + 롱 폴링.
 *
 * <p>시작할 때 상태 저장소에서 마지막으로 소비한 오프셋을 읽어 그 다음부터 가져온다.
 * {@link #commit()} 을 처리 전에 부르면 최대 한 번, 처리 후에 부르면 최소 한 번이 된다.
 *
 * <p>1단계는 소비자 그룹 코디네이터가 없다. 맡을 파티션을 설정으로 정한다(비우면 전부).
 * 4단계에서 코디네이터와 재조정이 이 자리를 대신한다.
 */
public final class Consumer implements AutoCloseable {

    public record Config(String bootstrap, String group, String topic, List<Integer> partitions, int maxRecords, long waitMs) {
    }

    private final Config cfg;
    private final MiniMqHttp http;
    private final List<Integer> assigned;
    private final Map<Integer, Long> position = new ConcurrentHashMap<>(); // 다음에 읽을 오프셋
    private final Map<Integer, Long> logEnd = new ConcurrentHashMap<>();
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public Consumer(Config cfg) throws IOException {
        this.cfg = cfg;
        this.http = new MiniMqHttp(cfg.bootstrap());
        List<TopicInfo> topics = http.get("/topics", new TypeReference<List<TopicInfo>>() {}, Duration.ofSeconds(5));
        int n = topics.stream().filter(t -> t.name().equals(cfg.topic())).findFirst()
                .orElseThrow(() -> new IOException("없는 토픽: " + cfg.topic())).partitions();
        this.assigned = cfg.partitions() == null || cfg.partitions().isEmpty()
                ? IntStream.range(0, n).boxed().toList()
                : cfg.partitions();
        for (int p : assigned) {
            long committed = http.get("/offsets?group=%s&topic=%s&partition=%d".formatted(enc(cfg.group()), enc(cfg.topic()), p),
                    new TypeReference<Long>() {}, Duration.ofSeconds(5));
            position.put(p, committed + 1);
        }
    }

    /** 맡은 파티션 모두에 동시에 롱 폴링을 걸고, 받은 것을 모아 돌려준다. */
    public List<Message> poll() throws IOException {
        List<Future<FetchResponse>> futures = new ArrayList<>();
        for (int p : assigned) {
            String path = "/fetch?topic=%s&partition=%d&offset=%d&max=%d&waitMs=%d"
                    .formatted(enc(cfg.topic()), p, position.get(p), cfg.maxRecords(), cfg.waitMs());
            futures.add(pool.submit(() -> http.get(path, new TypeReference<FetchResponse>() {},
                    Duration.ofMillis(cfg.waitMs() + 5_000))));
        }
        List<Message> out = new ArrayList<>();
        for (Future<FetchResponse> f : futures) {
            FetchResponse res;
            try {
                res = f.get();
            } catch (Exception e) {
                throw new IOException("fetch 실패", e);
            }
            int p = res.partition();
            logEnd.put(p, res.logEndOffset());
            long pos = position.get(p);
            if (pos < res.logStartOffset()) pos = res.logStartOffset(); // 보관 기간이 지나 지워진 구간은 건너뜀
            if (!res.messages().isEmpty()) pos = res.messages().get(res.messages().size() - 1).offset() + 1;
            position.put(p, pos);
            out.addAll(res.messages());
        }
        return out;
    }

    /** 지금까지 poll 로 받은 위치까지 커밋한다. */
    public void commit() throws IOException {
        for (int p : assigned) {
            long last = position.get(p) - 1;
            if (last >= 0) http.post("/offsets", new OffsetCommit(cfg.group(), cfg.topic(), p, last), Void.class);
        }
    }

    /** 파티션별 lag = 로그 끝 - 다음에 읽을 위치. */
    public Map<Integer, Long> lag() {
        Map<Integer, Long> out = new TreeMap<>();
        for (int p : assigned) out.put(p, Math.max(0, logEnd.getOrDefault(p, 0L) - position.get(p)));
        return out;
    }

    public List<Integer> assigned() {
        return assigned;
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
