package study.minimq.demo;

import java.time.Instant;
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
 * S1 로그창고 흉내: 서버 N대가 로그 한 줄씩 보낸다. 키 = 서버 ID.
 * 값에 생산자 실행 ID(run)와 서버별 일련번호(seq)를 넣어 소비자가 순서·중복·유실을 검사할 수 있게 한다.
 */
@Component
@ConditionalOnProperty(name = "demo.role", havingValue = "producer")
class ProducerRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(ProducerRunner.class);
    private static final String[] PATHS = {"/api/orders", "/api/users", "/api/search", "/health", "/api/cart"};
    private static final int[] STATUS = {200, 200, 200, 200, 201, 404, 500};

    private final DemoProperties props;

    ProducerRunner(DemoProperties props) {
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Startup.waitForBroker(props.bootstrap());
        new MiniMqHttp(props.bootstrap()).post("/topics", new TopicInfo(props.topic(), props.topicPartitions()), TopicInfo.class);

        Producer.Config cfg = new Producer.Config(props.bootstrap(), props.batchSize(), props.lingerMs(), props.acks(), 5);
        long[] seq = new long[props.servers()];
        String run = Long.toHexString(System.nanoTime() & 0xffffff); // 생산자가 재시작하면 seq 가 1부터 다시 시작하므로 구분용
        long intervalNanos = 1_000_000_000L / Math.max(1, props.ratePerSec());
        log.info("producer 시작 topic={} servers={} rate={}/s batch={} linger={}ms acks={}",
                props.topic(), props.servers(), props.ratePerSec(), props.batchSize(), props.lingerMs(), props.acks());

        try (Producer producer = new Producer(cfg)) {
            long next = System.nanoTime();
            long lastReport = System.currentTimeMillis();
            while (!Thread.currentThread().isInterrupted()) {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                int server = r.nextInt(props.servers());
                long s = ++seq[server];
                String key = "server-%02d".formatted(server);
                String value = "%s %s run=%s seq=%d GET %s %d %dms".formatted(Instant.now(), key, run, s,
                        PATHS[r.nextInt(PATHS.length)], STATUS[r.nextInt(STATUS.length)], r.nextInt(5, 300));
                producer.send(props.topic(), key, value)
                        .exceptionally(e -> {
                            log.warn("전송 실패 {} seq={}: {}", key, s, e.getMessage());
                            return -1L;
                        });

                next += intervalNanos;
                long wait = next - System.nanoTime();
                if (wait > 0) LockSupport.parkNanos(wait);

                if (System.currentTimeMillis() - lastReport >= 5_000) {
                    lastReport = System.currentTimeMillis();
                    log.info("[producer] {}", producer.stats());
                }
            }
        }
    }
}
