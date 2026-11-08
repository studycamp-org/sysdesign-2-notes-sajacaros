package study.minimq.broker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.stereotype.Component;

import study.minimq.broker.raft.RaftNode;
import study.minimq.client.MiniMqHttp;

/**
 * 메시지 한 건 추적. 키가 "trace-" 로 시작하는 메시지가 지나가는 단계를 이 브로커에서 일어난 만큼 기록한다.
 * 대시보드가 모든 브로커의 기록을 모아 시간 순으로 보여 준다.
 *
 * <p>리더가 저장할 때 추적을 등록하고(오프셋을 그때 안다), 다른 브로커에도 알린다
 * (오프셋 커밋은 그룹 코디네이터 브로커에서 일어나니까).
 */
@Component
public class Traces {
    public static final String PREFIX = "trace-";
    private static final long KEEP_MS = 10 * 60_000;

    public record Trace(String id, String topic, int partition, long offset, long registeredAt) {
    }

    /** seq: 이 브로커 안에서의 순서. 같은 밀리초에 일어난 단계의 순서를 지킨다. */
    public record Event(String trace, String step, String who, String text, long at, long seq, int broker) {
    }

    private final BrokerProperties props;
    private final RaftNode raft;
    private final Map<String, Trace> traces = new LinkedHashMap<>();
    private final List<Event> events = new ArrayList<>();
    private final Set<String> fired = new HashSet<>();
    private long seq;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public Traces(BrokerProperties props, RaftNode raft) {
        this.props = props;
        this.raft = raft;
    }

    /** 이 브로커에서 추적을 시작하고 다른 브로커에도 알린다. */
    public void registerAndBroadcast(Trace t) {
        register(t);
        raft.members().forEach((id, url) -> {
            if (id == props.brokerId()) return;
            pool.submit(() -> {
                try {
                    new MiniMqHttp(url).post("/admin/trace/register", t, Void.class, Duration.ofSeconds(2));
                } catch (Exception ignored) {
                }
            });
        });
    }

    public synchronized void register(Trace t) {
        long now = System.currentTimeMillis();
        traces.values().removeIf(x -> now - x.registeredAt() > KEEP_MS);
        events.removeIf(e -> now - e.at() > KEEP_MS);
        traces.putIfAbsent(t.id(), t);
    }

    /** 단계 하나를 기록한다 (추적과 무관한 단계는 같은 trace 로 한 번만). */
    public synchronized void record(String traceId, String step, String who, String text) {
        if (!fired.add(traceId + "|" + step + "|" + who)) return;
        events.add(new Event(traceId, step, who, text, System.currentTimeMillis(), ++seq, props.brokerId()));
    }

    /** 브로커 밖(생산자)에서 일어난 단계. broker 칸은 0, 시각·순서는 생산자가 잰 값. */
    public synchronized void recordFromClient(String traceId, String step, String who, String text, long at, long clientSeq) {
        if (!fired.add(traceId + "|" + step + "|" + who)) return;
        events.add(new Event(traceId, step, who, text, at, clientSeq, 0));
    }

    /** topic-partition 의 [from, to) 구간이 추적 중인 메시지를 지나갔으면 기록한다. */
    public void hit(String topic, int partition, long from, long to, String step, String who, String text) {
        List<Trace> matched = new ArrayList<>();
        synchronized (this) {
            if (traces.isEmpty()) return;
            for (Trace t : traces.values()) {
                if (t.topic().equals(topic) && t.partition() == partition && from <= t.offset() && t.offset() < to) matched.add(t);
            }
        }
        for (Trace t : matched) record(t.id(), step, who, text.replace("{offset}", Long.toString(t.offset())));
    }

    public synchronized Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("traces", new ArrayList<>(traces.values()));
        m.put("events", new ArrayList<>(events));
        return m;
    }
}
