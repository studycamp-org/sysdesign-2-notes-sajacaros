package study.minimq.broker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * 대시보드의 흐름 그림용: 어떤 클라이언트가 이 브로커에 무엇을 얼마나 보내고 가져가는지.
 * kind = produce | fetch | commit | metadata. 최근 3초 평균 건수/초.
 */
@Component
public class Traffic {
    private static final long WINDOW_MS = 3_000;
    private static final long FORGET_MS = 10_000;

    private record Key(String client, String kind, String topic, int partition) {
    }

    private static final class Counter {
        final Deque<long[]> samples = new ArrayDeque<>(); // [시각, 건수]
        long lastSeen;
        String detail;
    }

    private final Map<Key, Counter> counters = new HashMap<>();
    /** 누가 언제 GET /metadata 로 지도를 받아 갔나 (선출 과정 재생에서 "생산자가 지도를 받음" 단계로 쓴다). 최근 50건 */
    private final Deque<Map<String, Object>> metaFetches = new ArrayDeque<>();

    public synchronized void recordMetadataFetch(String client, long version, int controllerId, String summary) {
        if (client == null) return;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("client", client);
        m.put("at", System.currentTimeMillis());
        m.put("version", version);
        m.put("controllerId", controllerId);
        m.put("summary", summary);
        metaFetches.addLast(m);
        while (metaFetches.size() > 50) metaFetches.removeFirst();
    }

    public synchronized List<Map<String, Object>> metaFetches() {
        return new ArrayList<>(metaFetches);
    }

    public synchronized void record(String client, String kind, String topic, int partition, long count, String detail) {
        if (client == null) return;
        long now = System.currentTimeMillis();
        Counter c = counters.computeIfAbsent(new Key(client, kind, topic, partition), k -> new Counter());
        c.samples.addLast(new long[]{now, count});
        c.lastSeen = now;
        if (detail != null) c.detail = detail;
        while (!c.samples.isEmpty() && c.samples.peekFirst()[0] < now - WINDOW_MS) c.samples.removeFirst();
    }

    public synchronized List<Map<String, Object>> snapshot() {
        long now = System.currentTimeMillis();
        counters.values().removeIf(c -> now - c.lastSeen > FORGET_MS);
        List<Map<String, Object>> out = new ArrayList<>();
        counters.forEach((k, c) -> {
            while (!c.samples.isEmpty() && c.samples.peekFirst()[0] < now - WINDOW_MS) c.samples.removeFirst();
            long sum = 0;
            for (long[] s : c.samples) sum += s[1];
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("client", k.client());
            m.put("kind", k.kind());
            m.put("topic", k.topic());
            m.put("partition", k.partition());
            m.put("rate", Math.round(sum * 1000.0 / WINDOW_MS));
            m.put("reqRate", Math.round(c.samples.size() * 1000.0 / WINDOW_MS) == 0 && !c.samples.isEmpty() ? 1 : Math.round(c.samples.size() * 1000.0 / WINDOW_MS));
            m.put("agoMs", now - c.lastSeen);
            m.put("detail", c.detail);
            out.add(m);
        });
        return out;
    }
}
