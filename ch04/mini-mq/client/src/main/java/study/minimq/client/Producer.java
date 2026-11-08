package study.minimq.client;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.Partitioner;
import study.minimq.common.ProduceRecord;
import study.minimq.common.ProduceRequest;
import study.minimq.common.ProduceResponse;
import study.minimq.common.TopicInfo;

/**
 * 생산자 클라이언트 라이브러리 (그림 4.12): 라우팅과 버퍼를 생산자 안에 둔다.
 *
 * <ul>
 *   <li>라우팅: 키가 있으면 hash(key) % 파티션 수, 없으면 라운드 로빈</li>
 *   <li>버퍼: 파티션별로 모았다가 batchSize 가 차거나 lingerMs 가 지나면 한 번에 보낸다.
 *       묶음이 크면 대역폭이 늘고, 모으는 시간만큼 지연이 는다 (그림 4.13)</li>
 *   <li>순서: 파티션마다 전송 스레드를 하나만 둔다. 앞 묶음의 응답(재시도 포함)이 끝나야
 *       다음 묶음을 보내므로, 재시도해도 파티션 안 순서가 뒤집히지 않는다</li>
 *   <li>ACK=0 이면 응답을 기다리지도, 재시도하지도 않는다</li>
 * </ul>
 */
public final class Producer implements AutoCloseable {

    public record Config(String bootstrap, int batchSize, long lingerMs, int acks, int retries) {
        public static Config defaults(String bootstrap) {
            return new Config(bootstrap, 100, 20, 1, 5);
        }
    }

    private final Config cfg;
    private final MiniMqHttp http;
    private final Map<String, Integer> partitionCounts = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Batch> batches = new HashMap<>(); // this 로 보호
    private final Map<TopicPartition, ExecutorService> senders = new ConcurrentHashMap<>();
    private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
    private final AtomicLong roundRobin = new AtomicLong();

    private final AtomicLong sentRecords = new AtomicLong();
    private final AtomicLong sentBatches = new AtomicLong();
    private final AtomicLong failedRecords = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();

    public Producer(Config cfg) {
        this.cfg = cfg;
        this.http = new MiniMqHttp(cfg.bootstrap());
        long period = Math.max(1, cfg.lingerMs() / 2);
        ticker.scheduleAtFixedRate(this::flushLingered, period, period, TimeUnit.MILLISECONDS);
    }

    /** 버퍼에 넣고 바로 돌아온다. future 는 브로커가 붙인 오프셋으로 완료된다 (ACK=0 이면 -1). */
    public CompletableFuture<Long> send(String topic, String key, String value) {
        int n = partitions(topic);
        int p = key == null ? (int) Math.floorMod(roundRobin.getAndIncrement(), (long) n) : Partitioner.forKey(key, n);
        TopicPartition tp = new TopicPartition(topic, p);
        CompletableFuture<Long> f = new CompletableFuture<>();
        synchronized (this) {
            Batch b = batches.computeIfAbsent(tp, Batch::new);
            b.add(new ProduceRecord(key, value, System.currentTimeMillis()), f);
            if (b.records.size() >= cfg.batchSize()) {
                batches.remove(tp);
                dispatch(b);
            }
        }
        return f;
    }

    public synchronized void flush() {
        batches.values().forEach(this::dispatch);
        batches.clear();
    }

    private void flushLingered() {
        long now = System.currentTimeMillis();
        synchronized (this) {
            for (Iterator<Batch> it = batches.values().iterator(); it.hasNext(); ) {
                Batch b = it.next();
                if (now - b.createdAt >= cfg.lingerMs()) {
                    it.remove();
                    dispatch(b);
                }
            }
        }
    }

    /** 반드시 락을 잡은 채로 부른다. 그래야 같은 파티션의 묶음이 만든 순서대로 전송 큐에 들어간다. */
    private void dispatch(Batch b) {
        senders.computeIfAbsent(b.tp, tp -> Executors.newSingleThreadExecutor(Thread.ofVirtual().factory()))
                .submit(() -> sendWithRetry(b));
    }

    private void sendWithRetry(Batch b) {
        ProduceRequest req = new ProduceRequest(b.tp.topic(), b.tp.partition(), cfg.acks(), b.records);
        if (cfg.acks() == 0) {
            try {
                http.postAndForget("/produce", req);
            } catch (IOException ignored) {
                // 재시도 없음. 손실을 감수한다
            }
            sentBatches.incrementAndGet();
            sentRecords.addAndGet(b.records.size());
            b.futures.forEach(f -> f.complete(-1L));
            return;
        }
        IOException last = null;
        for (int attempt = 0; attempt <= cfg.retries(); attempt++) {
            try {
                ProduceResponse res = http.post("/produce", req, ProduceResponse.class);
                for (int i = 0; i < b.futures.size(); i++) b.futures.get(i).complete(res.baseOffset() + i);
                sentBatches.incrementAndGet();
                sentRecords.addAndGet(b.records.size());
                return;
            } catch (IOException e) {
                // 타임아웃이면 브로커는 이미 저장했을 수도 있다 → 재시도하면 중복 (최소 한 번)
                last = e;
                retries.incrementAndGet();
                sleep(100L << Math.min(attempt, 5)); // 지수형 백오프
            }
        }
        failedRecords.addAndGet(b.records.size());
        for (CompletableFuture<Long> f : b.futures) f.completeExceptionally(last);
    }

    private int partitions(String topic) {
        return partitionCounts.computeIfAbsent(topic, t -> {
            try {
                List<TopicInfo> topics = http.get("/topics", new TypeReference<List<TopicInfo>>() {}, Duration.ofSeconds(5));
                return topics.stream().filter(x -> x.name().equals(t)).findFirst()
                        .orElseThrow(() -> new IllegalStateException("없는 토픽: " + t)).partitions();
            } catch (IOException e) {
                throw new IllegalStateException("메타데이터를 못 가져옴", e);
            }
        });
    }

    public String stats() {
        return "sent=%d batches=%d (평균 %.1f건/묶음) retries=%d failed=%d".formatted(
                sentRecords.get(), sentBatches.get(),
                sentBatches.get() == 0 ? 0.0 : (double) sentRecords.get() / sentBatches.get(),
                retries.get(), failedRecords.get());
    }

    @Override
    public void close() {
        ticker.shutdownNow();
        flush();
        senders.values().forEach(ExecutorService::shutdown);
        for (ExecutorService s : senders.values()) {
            try {
                s.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class Batch {
        final TopicPartition tp;
        final long createdAt = System.currentTimeMillis();
        final List<ProduceRecord> records = new ArrayList<>();
        final List<CompletableFuture<Long>> futures = new ArrayList<>();

        Batch(TopicPartition tp) {
            this.tp = tp;
        }

        void add(ProduceRecord r, CompletableFuture<Long> f) {
            records.add(r);
            futures.add(f);
        }
    }
}
