package study.minimq.broker;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param segmentBytes    활성 세그먼트가 이 크기를 넘으면 새 세그먼트를 연다
 * @param retentionMs     보관 기간. 교재는 2주, 데모는 짧게 잡아 세그먼트가 지워지는 걸 본다
 * @param flushEveryWrite true 면 쓸 때마다 fsync. false 면 OS 페이지 캐시에 맡긴다(교재 기본)
 */
@ConfigurationProperties(prefix = "minimq")
public record BrokerProperties(
        @DefaultValue("1") int brokerId,
        @DefaultValue("./data") String dataDir,
        @DefaultValue("1048576") long segmentBytes,
        @DefaultValue("1209600000") long retentionMs,
        @DefaultValue("false") boolean flushEveryWrite) {
}
