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
        // 같은 그룹에 여러 컨테이너가 뜨므로 이름 끝에 컨테이너마다 다른 꼬리를 붙인다
        String host = System.getenv().getOrDefault("HOSTNAME", Long.toHexString(System.nanoTime()));
        String clientId = "consumer-" + props.group() + "-" + host.substring(0, Math.min(4, host.length()));
        MiniMqHttp.setClientId(clientId);
        Startup.waitForTopic(props.bootstrap(), props.topic());
        boolean commitBefore = "before".equalsIgnoreCase(props.commitMode());
        Consumer.Config cfg = new Consumer.Config(props.bootstrap(), props.group(), props.topic(),
                props.partitions(), props.maxRecords(), props.waitMs(), props.assignor());
        // (파티션|키)별로 본 적 있는 가장 큰 seq. 소비자를 다시 만들어도 유지한다.
        // 그룹에서는 파티션이 다른 멤버에게 갔다 돌아올 수 있으므로, 새로 받은 파티션의 기록은 지운다
        Map<String, String> lastRun = new HashMap<>();
        Map<String, Long> maxSeq = new HashMap<>();
        java.util.Set<Integer> owned = new java.util.HashSet<>();
        Consumer.AssignmentListener onAssigned = (gen, member, leader, parts, plan) -> {
            for (int p : parts) {
                if (owned.contains(p)) continue;
                String prefix = p + "|";
                maxSeq.keySet().removeIf(k -> k.startsWith(prefix));
                lastRun.keySet().removeIf(k -> k.startsWith(prefix));
            }
            owned.clear();
            owned.addAll(parts);
            log.info(
                "배정 받음: 세대 {} {} 파티션 {}{}", gen, member.substring(member.lastIndexOf('#')),
                parts.isEmpty() ? "없음 (파티션보다 소비자가 많아 논다)" : parts,
                leader ? "  ← 내가 리더 소비자, 계획 " + plan.values() : "");
        };

        long consumed = 0, gaps = 0, dups = 0, badCrc = 0, crashes = 0, sinceReport = 0;
        long lastReport = System.currentTimeMillis();
        long lastCrash = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                owned.clear(); // 새로 만든 소비자는 아무 파티션도 갖고 있지 않다. 그 사이 다른 멤버가 소비한 구간을 gap 으로 세지 않게
                try (Consumer consumer = new Consumer(cfg, clientId, onAssigned)) {
                    log.info("consumer 시작 {} group={} commit={} ({}) processMs={} crashEverySec={}",
                            clientId, props.group(), props.commitMode(),
                            commitBefore ? "최대 한 번" : "최소 한 번", props.processMs(), props.crashEverySec());
                    boolean crashed = false;
                    while (!crashed && !Thread.currentThread().isInterrupted()) {
                        String cmd = consumer.takeCommand();
                        if ("crash".equals(cmd)) {
                            // 그림 4.20: 그룹에 알리지 않고 멈춘다 → 코디네이터는 박동이 끊긴 뒤에야 안다
                            log.warn("⏸ 대시보드 지시: 15초 동안 멈춤 (박동도 끊김)");
                            consumer.abandon();
                            Thread.sleep(15_000);
                            break;
                        }
                        if ("leave".equals(cmd)) {
                            // 그림 4.19: 나간다고 알리고 나간다 → 바로 재조정
                            log.warn("👋 대시보드 지시: 그룹에서 나감, 10초 뒤 다시 합류");
                            consumer.close();
                            Thread.sleep(10_000);
                            break;
                        }
                        List<Message> batch;
                        try {
                            batch = consumer.poll();
                        } catch (IOException e) {
                            // 브로커·코디네이터가 잠깐 안 될 때: 그룹에서 나가지 말고(나가면 재조정이 두 번) 같은 소비자로 다시 시도
                            log.warn("poll 실패, 1초 뒤 다시: {}", e.toString());
                            Thread.sleep(1_000);
                            continue;
                        }
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
                                String k = m.partition() + "|" + m.key();
                                if (!mm.group(1).equals(lastRun.put(k, mm.group(1)))) maxSeq.remove(k);
                                Long max = maxSeq.get(k);
                                if (max != null && s <= max) {
                                    dups++; // 이미 본 것을 다시 봤다
                                } else {
                                    if (max != null && s > max + 1) gaps += s - max - 1; // 건너뛴 건수만큼 유실
                                    maxSeq.put(k, s);
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
                            log.info("[consumer {} {} 세대 {}] consumed={} ({}/s) lag={} gap={} dup={} crash={} badCrc={}{}",
                                    props.group(), props.topic(), consumer.generation(), consumed, Math.round(rate), consumer.lag(), gaps, dups, crashes, badCrc,
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
