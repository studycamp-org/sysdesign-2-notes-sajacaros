package study.minimq.demo;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 환경 변수로 바꾼다. 예) DEMO_ROLE=consumer DEMO_GROUP=search DEMO_COMMIT_MODE=before
 *
 * @param role        producer | consumer
 * @param servers     로그를 보내는 가상 서버 수 (키 = server-NN)
 * @param ratePerSec  초당 생산 건수
 * @param commitMode  before = 처리 전 커밋(최대 한 번) / after = 처리 후 커밋(최소 한 번)
 * @param processMs   메시지 한 건 처리에 걸리는 시간 (소비자를 일부러 느리게)
 * @param partitions  이 소비자가 맡을 파티션 (비우면 전부)
 * @param crashEverySec 0 보다 크면 그 주기마다 "묶음 처리 도중 죽음"을 흉내 낸다.
 *                     소비자를 버리고 커밋된 오프셋부터 다시 시작 → before 면 gap(유실), after 면 dup(중복)
 */
@ConfigurationProperties(prefix = "demo")
public record DemoProperties(
        @DefaultValue("producer") String role,
        @DefaultValue("http://localhost:8080") String bootstrap,
        @DefaultValue("logs") String topic,
        @DefaultValue("6") int topicPartitions,
        @DefaultValue("20") int servers,
        @DefaultValue("200") int ratePerSec,
        @DefaultValue("50") int batchSize,
        @DefaultValue("20") long lingerMs,
        @DefaultValue("1") int acks,
        @DefaultValue("search") String group,
        @DefaultValue("after") String commitMode,
        @DefaultValue("0") long processMs,
        @DefaultValue("500") int maxRecords,
        @DefaultValue("1000") long waitMs,
        @DefaultValue("0") long crashEverySec,
        List<Integer> partitions) {
}
