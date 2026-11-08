package study.minimq.demo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import study.minimq.client.MiniMqHttp;
import study.minimq.client.Producer;
import study.minimq.common.TopicInfo;

/**
 * 서버 N대가 여러 토픽에 메시지를 보낸다. 토픽마다 acks 와 속도가 다르다
 * (예: 로그는 ACK=1 로 많이, 주문은 ACK=all 로, 지표는 ACK=0 으로).
 *
 * <p>키 = "이름-서버번호" (web-03). 값에 생산자 실행 ID(run)와 키별 일련번호(seq)를 넣어
 * 소비자가 순서·중복·유실을 검사할 수 있게 한다.
 */
@Component
@ConditionalOnProperty(name = "demo.role", havingValue = "producer")
class ProducerRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(ProducerRunner.class);
    private static final String[] PATHS = {"/api/orders", "/api/users", "/api/search", "/health", "/api/cart"};
    private static final int[] STATUS = {200, 200, 200, 200, 201, 404, 500};

    /** 토픽 하나에 보내는 설정. */
    record Spec(String topic, int partitions, String acks, int minInsyncReplicas, int ratePerSec) {
        int acksValue() {
            return "all".equalsIgnoreCase(acks) ? -1 : Integer.parseInt(acks);
        }

        static Spec parse(String s) {
            String[] f = s.trim().split(":");
            if (f.length != 5) throw new IllegalArgumentException("DEMO_TOPICS 항목은 토픽:파티션수:acks:최소ISR:초당건수 — " + s);
            return new Spec(f[0], Integer.parseInt(f[1]), f[2], Integer.parseInt(f[3]), Integer.parseInt(f[4]));
        }
    }

    private final DemoProperties props;

    ProducerRunner(DemoProperties props) {
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<Spec> specs = new ArrayList<>();
        if (props.topics().isBlank()) {
            specs.add(new Spec(props.topic(), props.topicPartitions(), props.acks(), props.minInsyncReplicas(), props.ratePerSec()));
        } else {
            for (String s : props.topics().split(",")) if (!s.isBlank()) specs.add(Spec.parse(s));
        }
        MiniMqHttp.setClientId("producer-" + props.name());
        String run = Long.toHexString(System.nanoTime() & 0xffffff); // 생산자가 재시작하면 seq 가 1부터 다시 시작하므로 구분용

        List<Thread> threads = new ArrayList<>();
        for (Spec spec : specs) {
            Startup.createTopic(props.bootstrap(), new TopicInfo(spec.topic(), spec.partitions(),
                    props.replicationFactor(), spec.minInsyncReplicas()));
            threads.add(Thread.ofVirtual().name("produce-" + spec.topic()).start(() -> produce(spec, run)));
        }
        for (Thread t : threads) t.join();
    }

    /** 토픽 하나에 정해진 속도로 계속 보낸다. 생산자 객체는 토픽마다 따로(acks 가 다르니까). */
    private void produce(Spec spec, String run) {
        // 재시도를 끝없이 한다(ACK=0 빼고): 생산자가 "실패"로 포기하는 메시지가 없으므로,
        // 소비자가 보는 gap 은 전부 "응답(ACK)까지 받았는데 사라진" 메시지다
        Producer.Config cfg = new Producer.Config(props.bootstrap(), props.batchSize(), props.lingerMs(), spec.acksValue(), Integer.MAX_VALUE);
        long[] seq = new long[props.servers()];
        long intervalNanos = 1_000_000_000L / Math.max(1, spec.ratePerSec());
        log.info("producer-{} → {} 시작: {}건/초 acks={} 최소ISR={} (키 {}-00 ~ {}-{})", props.name(), spec.topic(),
                spec.ratePerSec(), spec.acks(), spec.minInsyncReplicas(), props.name(), props.name(), props.servers() - 1);

        try (Producer producer = new Producer(cfg)) {
            long next = System.nanoTime();
            long lastReport = System.currentTimeMillis();
            while (!Thread.currentThread().isInterrupted()) {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                int server = r.nextInt(props.servers());
                long s = ++seq[server];
                String key = "%s-%02d".formatted(props.name(), server);
                String value = "%s %s run=%s seq=%d GET %s %d %dms".formatted(Instant.now(), key, run, s,
                        PATHS[r.nextInt(PATHS.length)], STATUS[r.nextInt(STATUS.length)], r.nextInt(5, 300));
                producer.send(spec.topic(), key, value)
                        .exceptionally(e -> {
                            log.warn("전송 실패 {} seq={}: {}", key, s, e.getMessage());
                            return -1L;
                        });

                next += intervalNanos;
                long wait = next - System.nanoTime();
                if (wait > 0) LockSupport.parkNanos(wait);

                if (System.currentTimeMillis() - lastReport >= 5_000) {
                    lastReport = System.currentTimeMillis();
                    log.info("[producer {} → {} acks={}] {}", props.name(), spec.topic(), spec.acks(), producer.stats());
                }
            }
        }
    }
}
