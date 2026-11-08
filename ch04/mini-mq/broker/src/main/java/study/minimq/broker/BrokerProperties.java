package study.minimq.broker;

import java.util.Map;
import java.util.TreeMap;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param producers       대시보드가 다룰 생산자 "producer-logs=http://producer-logs:8080,..." (대시보드용)
 * @param peers           클러스터 구성원 "1=http://broker-1:8080,2=..." (나 포함). 고정 멤버십
 * @param segmentBytes    활성 세그먼트가 이 크기를 넘으면 새 세그먼트를 연다
 * @param retentionMs     보관 기간. 교재는 2주, 데모는 짧게 잡아 세그먼트가 지워지는 걸 본다
 * @param flushEveryWrite true 면 쓸 때마다 fsync. false 면 OS 페이지 캐시에 맡긴다(교재 기본)
 * @param electionMinMs   선거 타임아웃 하한. 이 시간 동안 리더 박동이 없으면 출마한다
 * @param electionMaxMs   선거 타임아웃 상한. 노드마다 무작위로 골라 동시에 출마하는 일을 줄인다
 * @param heartbeatMs     리더(컨트롤러)의 박동 주기
 * @param brokerDeadMs    컨트롤러가 이 시간 동안 응답을 못 받으면 그 브로커를 죽음으로 표시한다
 * @param replicaLagMaxMessages 리더보다 이만큼 넘게 뒤처지면 ISR 에서 뺀다 (교재 replica.lag.max.messages)
 * @param replicaLagTimeMs      이 시간 동안 가져가러 오지 않아도 ISR 에서 뺀다 (카프카 replica.lag.time.max.ms)
 * @param ackTimeoutMs          ACK=all 생산자가 ISR 전원의 복제를 기다리는 최대 시간
 * @param sessionTimeoutMs      소비자 박동이 이 시간 동안 없으면 죽은 것으로 보고 재조정
 * @param rebalanceTimeoutMs    재조정 때 멤버가 다시 합류하기를 기다리는 최대 시간
 * @param initialRebalanceDelayMs 빈 그룹에 첫 멤버가 오면 다른 멤버도 함께 오도록 잠깐 기다린다
 */
@ConfigurationProperties(prefix = "minimq")
public record BrokerProperties(
        @DefaultValue("1") int brokerId,
        @DefaultValue("1=http://localhost:8080") String peers,
        @DefaultValue("") String producers,
        @DefaultValue("./data") String dataDir,
        @DefaultValue("1048576") long segmentBytes,
        @DefaultValue("1209600000") long retentionMs,
        @DefaultValue("false") boolean flushEveryWrite,
        @DefaultValue("1500") long electionMinMs,
        @DefaultValue("3000") long electionMaxMs,
        @DefaultValue("500") long heartbeatMs,
        @DefaultValue("3000") long brokerDeadMs,
        @DefaultValue("1000") long replicaLagMaxMessages,
        @DefaultValue("4000") long replicaLagTimeMs,
        @DefaultValue("5000") long ackTimeoutMs,
        @DefaultValue("6000") long sessionTimeoutMs,
        @DefaultValue("4000") long rebalanceTimeoutMs,
        @DefaultValue("1500") long initialRebalanceDelayMs) {

    public Map<String, String> producerUrls() {
        Map<String, String> m = new TreeMap<>();
        for (String part : producers.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length == 2) m.put(kv[0].trim(), kv[1].trim());
        }
        return m;
    }

    public Map<Integer, String> members() {
        Map<Integer, String> m = new TreeMap<>();
        for (String part : peers.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length == 2) m.put(Integer.parseInt(kv[0].trim()), kv[1].trim());
        }
        if (!m.containsKey(brokerId)) throw new IllegalStateException("peers 에 나(broker-" + brokerId + ")가 없다: " + peers);
        return m;
    }
}
