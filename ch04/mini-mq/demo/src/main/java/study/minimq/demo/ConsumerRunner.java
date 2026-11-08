package study.minimq.demo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import study.minimq.client.Consumer;
import study.minimq.client.MiniMqHttp;
import study.minimq.common.Message;

/**
 * 소비자. 서버별 seq 로 순서를 검사한다.
 * <ul>
 *   <li>seq 가 키별 최대값보다 2 이상 크면 gap (건너뛴 건수 = 유실 건수)</li>
 *   <li>seq 가 키별 최대값 이하면 dup (중복 소비. 처리 후 커밋에서 소비자가 죽었다 살아나면 생긴다)</li>
 * </ul>
 * 같은 키는 같은 파티션이므로 정상이라면 둘 다 0 이어야 한다.
 */
@Component
@ConditionalOnProperty(name = "demo.role", havingValue = "consumer")
class ConsumerRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(ConsumerRunner.class);
    private static final Pattern SEQ = Pattern.compile("run=(\\w+) seq=(\\d+)");

    private final DemoProperties props;

    ConsumerRunner(DemoProperties props) {
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        MiniMqHttp.setClientId("consumer-" + props.group());
        Startup.waitForTopic(props.bootstrap(), props.topic());
        boolean commitBefore = "before".equalsIgnoreCase(props.commitMode());
        Consumer.Config cfg = new Consumer.Config(props.bootstrap(), props.group(), props.topic(),
                props.partitions(), props.maxRecords(), props.waitMs());

        Map<String, String> lastRun = new HashMap<>();
        Map<String, Long> maxSeq = new HashMap<>(); // 키별로 본 적 있는 가장 큰 seq. 재시작(시뮬레이션)해도 유지한다
        long consumed = 0, gaps = 0, dups = 0, badCrc = 0, crashes = 0, sinceReport = 0;
        long lastReport = System.currentTimeMillis();
        long lastCrash = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                try (Consumer consumer = new Consumer(cfg)) {
                    log.info("consumer 시작 group={} partitions={} commit={} ({}) processMs={} crashEverySec={}",
                            props.group(), consumer.assigned(), props.commitMode(),
                            commitBefore ? "최대 한 번" : "최소 한 번", props.processMs(), props.crashEverySec());
                    boolean crashed = false;
                    while (!crashed && !Thread.currentThread().isInterrupted()) {
                        List<Message> batch = consumer.poll();
                        if (commitBefore) consumer.commit(); // 처리 전에 커밋 → 처리 중 죽으면 그 묶음은 유실

                        boolean crashNow = props.crashEverySec() > 0 && batch.size() >= 2
                                && System.currentTimeMillis() - lastCrash >= props.crashEverySec() * 1000;
                        for (int i = 0; i < batch.size(); i++) {
                            if (crashNow && i == batch.size() / 2) {
                                crashed = true;
                                break;
                            }
                            Message m = batch.get(i);
                            if (!m.crcValid()) badCrc++;
                            Matcher mm = SEQ.matcher(m.value());
                            if (mm.find()) {
                                long s = Long.parseLong(mm.group(2));
                                // 생산자가 바뀌었으면(run 이 다르면) seq 를 새로 센다
                                if (!mm.group(1).equals(lastRun.put(m.key(), mm.group(1)))) maxSeq.remove(m.key());
                                Long max = maxSeq.get(m.key());
                                if (max != null && s <= max) {
                                    dups++; // 이미 본 것을 다시 봤다
                                } else {
                                    if (max != null && s > max + 1) gaps += s - max - 1; // 건너뛴 건수만큼 유실
                                    maxSeq.put(m.key(), s);
                                }
                            }
                            if (props.processMs() > 0) Thread.sleep(props.processMs());
                            consumed++;
                            sinceReport++;
                        }
                        if (crashed) {
                            crashes++;
                            lastCrash = System.currentTimeMillis();
                            log.warn("💥 묶음 {}건 중 {}건 처리하고 죽음 (시뮬레이션) → 커밋된 오프셋부터 다시 시작",
                                    batch.size(), batch.size() / 2);
                            break;
                        }

                        if (!commitBefore) consumer.commit(); // 처리 후에 커밋 → 커밋 전에 죽으면 다시 읽어 중복

                        long now = System.currentTimeMillis();
                        if (now - lastReport >= 5_000) {
                            double rate = sinceReport * 1000.0 / (now - lastReport);
                            log.info("[consumer {} {}] consumed={} ({}/s) lag={} gap={} dup={} crash={} badCrc={}{}",
                                    props.group(), props.topic(), consumed, Math.round(rate), consumer.lag(), gaps, dups, crashes, badCrc,
                                    consumer.unavailable().isEmpty() ? "" : " 리더없음=" + consumer.unavailable());
                            lastReport = now;
                            sinceReport = 0;
                        }
                    }
                }
            } catch (IOException e) {
                // 브로커가 내려가 있으면 프로세스를 죽이지 말고 다시 붙는다 (카운터를 잃지 않으려고)
                log.warn("브로커 연결 실패, 1초 뒤 커밋된 오프셋부터 다시 시작: {}", e.toString());
                Thread.sleep(1_000);
            }
        }
    }
}
