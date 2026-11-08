package study.minimq.client;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.FetchResponse;
import study.minimq.common.Message;
import study.minimq.common.OffsetCommit;

/**
 * 소비자 클라이언트 (그림 4.14, 4.15). 풀 모델 + 롱 폴링.
 *
 * <p>파티션마다 리더 브로커를 지도에서 찾아 그쪽에서 가져온다. 처음 읽을 때 그 리더에게서
 * 마지막으로 소비한 오프셋을 받아 그 다음부터 읽는다.
 * {@link #commit()} 을 처리 전에 부르면 최대 한 번, 처리 후에 부르면 최소 한 번이 된다.
 *
 * <p>리더 브로커가 죽은 파티션은 건너뛰고 나머지만 읽는다({@link #unavailable()}).
 * 2단계는 사본이 없어 그 파티션은 브로커가 살아날 때까지 멈춘다.
 *
 * <p>소비자 그룹 코디네이터가 아직 없다. 맡을 파티션을 설정으로 정한다(비우면 전부). 4단계에서 바뀐다.
 */
public final class Consumer implements AutoCloseable {

    public record Config(String bootstrap, String group, String topic, List<Integer> partitions, int maxRecords, long waitMs) {
    }

    private final Config cfg;
    private final ClusterView view;
    private final List<Integer> assigned;
    private final Map<Integer, Long> position = new ConcurrentHashMap<>(); // 다음에 읽을 오프셋. 없으면 아직 모름
    private final Map<Integer, Long> logEnd = new ConcurrentHashMap<>();
    private final Set<Integer> unavailable = ConcurrentHashMap.newKeySet();
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public Consumer(Config cfg) throws IOException {
        this.cfg = cfg;
        this.view = new ClusterView(cfg.bootstrap());
        int n = view.partitions(cfg.topic());
        this.assigned = cfg.partitions() == null || cfg.partitions().isEmpty()
                ? IntStream.range(0, n).boxed().toList()
                : cfg.partitions();
    }

    /** 맡은 파티션 모두에 동시에 롱 폴링을 걸고, 받은 것을 모아 돌려준다. 모든 파티션이 실패하면 예외. */
    public List<Message> poll() throws IOException {
        Map<Integer, Future<FetchResponse>> futures = new TreeMap<>();
        for (int p : assigned) futures.put(p, pool.submit(() -> fetch(p)));

        List<Message> out = new ArrayList<>();
        IOException lastError = null;
        for (Map.Entry<Integer, Future<FetchResponse>> e : futures.entrySet()) {
            int p = e.getKey();
            FetchResponse res;
            try {
                res = e.getValue().get();
                unavailable.remove(p);
            } catch (Exception ex) {
                unavailable.add(p);
                view.invalidate();
                lastError = new IOException("파티션 " + p + " 을 못 읽음", ex.getCause() != null ? ex.getCause() : ex);
                continue;
            }
            logEnd.put(p, res.logEndOffset());
            long pos = position.get(p);
            if (pos < res.logStartOffset()) pos = res.logStartOffset(); // 보관 기간이 지나 지워진 구간은 건너뜀
            if (!res.messages().isEmpty()) pos = res.messages().get(res.messages().size() - 1).offset() + 1;
            position.put(p, pos);
            out.addAll(res.messages());
        }
        if (unavailable.size() == assigned.size() && lastError != null) throw lastError;
        return out;
    }

    private FetchResponse fetch(int p) throws IOException {
        MiniMqHttp leader = view.leader(cfg.topic(), p);
        Long pos = position.get(p);
        if (pos == null) {
            long committed = leader.get("/offsets?group=%s&topic=%s&partition=%d".formatted(enc(cfg.group()), enc(cfg.topic()), p),
                    new TypeReference<Long>() {}, Duration.ofSeconds(2));
            pos = committed + 1;
            position.put(p, pos);
        }
        String path = "/fetch?topic=%s&partition=%d&offset=%d&max=%d&waitMs=%d"
                .formatted(enc(cfg.topic()), p, pos, cfg.maxRecords(), cfg.waitMs());
        return leader.get(path, new TypeReference<FetchResponse>() {}, Duration.ofMillis(cfg.waitMs() + 5_000));
    }

    /** 지금까지 poll 로 받은 위치까지 커밋한다. 리더가 죽은 파티션은 건너뛴다. */
    public void commit() {
        for (int p : assigned) {
            Long pos = position.get(p);
            if (pos == null || pos == 0 || unavailable.contains(p)) continue;
            try {
                view.leader(cfg.topic(), p).post("/offsets", new OffsetCommit(cfg.group(), cfg.topic(), p, pos - 1), Void.class);
            } catch (IOException e) {
                unavailable.add(p);
                view.invalidate();
            }
        }
    }

    /** 파티션별 lag = 로그 끝 - 다음에 읽을 위치. 아직 모르는 파티션은 빠진다. */
    public Map<Integer, Long> lag() {
        Map<Integer, Long> out = new TreeMap<>();
        for (int p : assigned) {
            Long pos = position.get(p);
            Long end = logEnd.get(p);
            if (pos != null && end != null) out.put(p, Math.max(0, end - pos));
        }
        return out;
    }

    /** 리더에 닿지 못한 파티션. */
    public Set<Integer> unavailable() {
        return new TreeSet<>(unavailable);
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
