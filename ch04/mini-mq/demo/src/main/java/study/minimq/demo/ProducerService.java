package study.minimq.demo;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import jakarta.annotation.PostConstruct;
import study.minimq.client.ClusterView;
import study.minimq.client.MiniMqHttp;
import study.minimq.client.Producer;
import study.minimq.client.TopicPartition;
import study.minimq.common.ClusterMetadata;
import study.minimq.common.PartitionMetadata;
import study.minimq.common.TopicInfo;

/**
 * 생산자 하나 = 토픽 하나. 자동으로 보내지 않고, 대시보드 버튼(POST /send)을 받을 때만 보낸다.
 *
 * <p>클라이언트 라이브러리 그대로다 (그림 4.12): 보낸 메시지는 🧺 버퍼의 파티션별 묶음에 쌓이고,
 * 묶음이 batchSize(5)건이 되면 🧭 라우팅이 지도에서 리더를 찾아 한 번에 보낸다. 시간으로는 보내지 않는다(lingerMs 0).
 * 덜 찬 묶음은 대시보드의 "지금 보내기"(POST /flush)로 내보낸다.
 * 보내는 메시지는 키가 trace- 로 시작해서, 생산자 안에서 일어난 단계를 브로커의 추적 기록에 남긴다.
 */
@RestController
@ConditionalOnProperty(name = "demo.role", havingValue = "producer")
class ProducerService {
    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private final DemoProperties props;
    private final String name;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong eventSeq = new AtomicLong();
    private volatile Producer producer;
    private volatile long producerSince; // 지금 생산자를 만든 시각 (켜짐·초기화). 그 전의 지도 요청은 이 생산자의 것이 아니다
    private ClusterView brokers;

    ProducerService(DemoProperties props) {
        this.props = props;
        this.name = "producer-" + props.name();
    }

    @PostConstruct
    void start() {
        MiniMqHttp.setClientId(name);
        brokers = new ClusterView(props.bootstrap());
        // 토픽이 만들어질 때까지 기다렸다가 생산자를 만든다 (요청은 그 전엔 503)
        Thread.ofVirtual().start(() -> {
            // 토픽 만들기는 관리 작업이다 (kafka-topics.sh 처럼). 생산자 이름이 아닌 admin 으로 요청한다
            MiniMqHttp.setClientId("admin-" + props.name());
            try {
                Startup.createTopic(props.bootstrap(), new TopicInfo(props.topic(), props.topicPartitions(),
                        props.replicationFactor(), props.minInsyncReplicas()));
            } catch (InterruptedException e) {
                return;
            }
            MiniMqHttp.setClientId(name);
            producer = newProducer();
            log.info("{} 준비: {} 로 보냄, acks={}, 묶음 {}건 또는 {}ms", name, props.topic(), props.acks(), props.batchSize(), props.lingerMs());
        });
    }

    /** 지도도 버퍼도 없는 새 생산자 (켜진 직후 상태). */
    private Producer newProducer() {
        producerSince = System.currentTimeMillis();
        // 생산자는 지도 없이 시작한다. 처음 send() 할 때 GET /metadata 로 받는다 (실제 Kafka 와 같다)
        Producer p = new Producer(new Producer.Config(props.bootstrap(), props.batchSize(), props.lingerMs(), props.acksValue(), 5));
        p.setListener(new Producer.Listener() {
            @Override
            public void buffered(String key, TopicPartition tp, int partitions, int batchSizeNow) {
                event(key, "buffered", "🧺 " + name + " 버퍼: hash(key) % " + partitions + " = 파티션 " + tp.partition()
                        + " → " + tp.topic() + "-" + tp.partition() + " 묶음에 담음 (" + batchSizeNow + "/" + props.batchSize()
                        + "건, " + (props.lingerMs() > 0 ? props.batchSize() + "건이 차거나 " + props.lingerMs() / 1000 + "초가 지나면 보냄)"
                        : props.batchSize() + "건이 차면 보냄)"));
            }

            @Override
            public void dispatched(TopicPartition tp, List<String> keys, int leaderId, long version, int attempt) {
                for (String key : keys) {
                    event(key, "dispatched", "🧭 " + name + " 라우팅: 지도 v" + version + " 에서 " + tp.topic() + "-" + tp.partition()
                            + " 의 리더 broker-" + leaderId + " 를 찾아 묶음 " + keys.size() + "건을 한 번에 보냄"
                            + (attempt > 0 ? " (재시도 " + attempt + ")" : ""));
                }
            }
        });
        return p;
    }

    /** 대시보드 버튼 "초기화": 지도·버퍼·카운터를 버리고 켜진 직후로 되돌린다. 버퍼에 있던 메시지는 버린다. */
    @PostMapping("/reset")
    public synchronized Map<String, Object> reset() {
        Producer old = producer;
        if (old == null) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "토픽을 준비하는 중");
        int dropped = old.buffered().stream().mapToInt(b -> b.keys().size()).sum();
        producer = newProducer();
        old.close();
        sent.set(0);
        acked.set(0);
        log.info("{} 초기화: 지도와 버퍼({}건)를 버림", name, dropped);
        return Map.of("producer", name, "dropped", dropped);
    }

    /** 대시보드 버튼: count 건을 보낸다. 버퍼에 담길 뿐, 실제 전송은 묶음이 차거나 시간이 지나야 일어난다. */
    @PostMapping("/send")
    public Map<String, Object> send(@RequestParam(defaultValue = "1") int count) throws IOException {
        Producer p = producer;
        if (p == null) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "토픽을 준비하는 중");
        boolean had = p.view().cached() != null;
        ClusterMetadata m = p.view().metadata(); // 지도가 없으면 여기서 처음 받는다
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < Math.max(1, Math.min(count, 50)); i++) {
            String id = "trace-" + Long.toString(System.nanoTime() % 2_000_000_000L, 36);
            ids.add(id);
            long t0 = System.currentTimeMillis();
            event(id, "map", "🗺 " + name + (had ? " 가 캐시한 지도" : " 는 지도가 없어 부트스트랩 브로커에서 처음 받음 (GET /metadata)") + ": 메타데이터 v" + m.version() + " — 컨트롤러 broker-" + m.controllerId()
                    + " 가 조정 서비스 /metadata 에 써 둔 사본 분산 계획. 메시지는 컨트롤러를 거치지 않는다");
            sent.incrementAndGet();
            p.send(props.topic(), id, "from " + name).whenComplete((offset, e) -> {
                if (e == null) {
                    acked.incrementAndGet();
                    event(id, "acked", "📬 " + name + " 가 ACK 받음 (acks=" + props.acks() + ", send() 부른 뒤 " + (System.currentTimeMillis() - t0) + "ms — 버퍼에서 기다린 시간 포함, offset "
                            + offset + ") — " + switch (props.acks()) {
                                case "0" -> "ACK=0 은 결과를 확인하지 않는다";
                                case "1" -> "리더가 저장하자마자 응답. 아직 복제 전일 수 있다";
                                default -> "ISR 전원이 받아 HW 가 넘어간 뒤 응답";
                            });
                } else {
                    event(id, "failed", "❌ " + name + " 전송 실패: " + e.getMessage());
                }
            });
        }
        return Map.of("producer", name, "topic", props.topic(), "ids", ids);
    }

    /** 대시보드 버튼: 덜 찬 묶음도 지금 보낸다. */
    @PostMapping("/flush")
    public Map<String, Object> flush() {
        Producer p = producer;
        if (p == null) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "토픽을 준비하는 중");
        int n = p.buffered().stream().mapToInt(b -> b.keys().size()).sum();
        p.flush();
        log.info("{} 지금 보내기: 버퍼의 {}건", name, n);
        return Map.of("producer", name, "flushed", n);
    }

    /** 대시보드용: 버퍼에 모여 있는 묶음, 캐시한 지도. */
    @GetMapping("/state")
    public Map<String, Object> state() throws IOException {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", name);
        out.put("topic", props.topic());
        out.put("acks", props.acks());
        out.put("batchSize", props.batchSize());
        out.put("lingerMs", props.lingerMs());
        out.put("sent", sent.get());
        out.put("acked", acked.get());
        Producer p = producer;
        if (p == null) {
            out.put("ready", false);
            return out;
        }
        out.put("ready", true);
        out.put("since", producerSince);
        ClusterMetadata m = p.view().cached(); // 대시보드가 묻는다고 지도를 받으러 가지 않는다
        if (m == null) {
            out.put("leaders", Map.of());
            out.put("buffer", p.buffered());
            return out;
        }
        out.put("metadataVersion", m.version());
        out.put("controllerId", m.controllerId());
        Map<Integer, Integer> leaders = new TreeMap<>();
        var t = m.topics().get(props.topic());
        if (t != null) for (PartitionMetadata pm : t.partitions()) leaders.put(pm.partition(), pm.leader());
        out.put("leaders", leaders);
        out.put("buffer", p.buffered());
        return out;
    }

    /** 생산자 안에서 일어난 단계를 브로커의 추적 기록에 남긴다 (아무 브로커나, 되는 곳에). */
    private void event(String trace, String step, String text) {
        if (trace == null || !trace.startsWith("trace-")) return;
        // 시각과 순서는 여기서 잰다 (비동기로 보내므로 브로커에 닿는 순서는 뒤바뀔 수 있다)
        Map<String, Object> body = Map.of("trace", trace, "step", step, "who", "client", "text", text,
                "at", System.currentTimeMillis(), "seq", eventSeq.incrementAndGet());
        pool.submit(() -> {
            for (String url : brokers.bootstraps()) {
                try {
                    brokers.http(url).post("/admin/trace/event", body, Void.class, Duration.ofSeconds(1));
                    return;
                } catch (IOException ignored) {
                    // 다음 브로커
                }
            }
        });
    }
}
