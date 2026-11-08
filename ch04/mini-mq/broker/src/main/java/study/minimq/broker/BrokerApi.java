package study.minimq.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import study.minimq.common.FetchResponse;
import study.minimq.common.OffsetCommit;
import study.minimq.common.ProduceRequest;
import study.minimq.common.ProduceResponse;
import study.minimq.common.TopicInfo;

@RestController
public class BrokerApi {
    private final BrokerProperties props;
    private final LogManager logs;
    private final OffsetStore offsets;

    public BrokerApi(BrokerProperties props, LogManager logs, OffsetStore offsets) {
        this.props = props;
        this.logs = logs;
        this.offsets = offsets;
    }

    @PostMapping("/topics")
    public TopicInfo createTopic(@RequestBody TopicInfo req) {
        return logs.createTopic(req);
    }

    @GetMapping("/topics")
    public Collection<TopicInfo> topics() {
        return logs.topics();
    }

    /** 생산자가 묶음을 보낸다. 1단계는 브로커 1대라 저장하면 바로 응답한다(사실상 ACK=1). */
    @PostMapping("/produce")
    public ProduceResponse produce(@RequestBody ProduceRequest req) {
        long[] range = logs.log(req.topic(), req.partition()).append(req.records());
        return new ProduceResponse(req.topic(), req.partition(), range[0], range[1]);
    }

    /**
     * 풀 모델 + 롱 폴링 (그림 4.14). offset 부터 최대 max 건을 묶어 준다.
     * 줄 게 없으면 waitMs 동안 기다렸다가, 그 사이 메시지가 들어오면 바로 응답한다.
     */
    @GetMapping("/fetch")
    public CompletableFuture<FetchResponse> fetch(@RequestParam String topic,
                                                 @RequestParam int partition,
                                                 @RequestParam long offset,
                                                 @RequestParam(defaultValue = "500") int max,
                                                 @RequestParam(defaultValue = "0") long waitMs) {
        PartitionLog log = logs.log(topic, partition);
        CompletableFuture<Void> ready = waitMs <= 0
                ? CompletableFuture.completedFuture(null)
                : log.awaitData(offset).completeOnTimeout(null, waitMs, TimeUnit.MILLISECONDS);
        return ready.thenApply(x -> new FetchResponse(topic, partition, log.read(offset, max),
                log.logStartOffset(), log.logEndOffset()));
    }

    @PostMapping("/offsets")
    public void commit(@RequestBody OffsetCommit c) {
        logs.log(c.topic(), c.partition()); // 없는 파티션이면 404
        offsets.commit(c);
    }

    @GetMapping("/offsets")
    public long committed(@RequestParam String group, @RequestParam String topic, @RequestParam int partition) {
        return offsets.committed(group, topic, partition);
    }

    /** 대시보드용 상태. */
    @GetMapping("/admin/state")
    public Map<String, Object> state() {
        List<PartitionState> partitions = logs.states();
        List<Map<String, Object>> groups = new ArrayList<>();
        for (OffsetCommit c : offsets.all()) {
            long leo = logs.log(c.topic(), c.partition()).logEndOffset();
            Map<String, Object> g = new LinkedHashMap<>();
            g.put("group", c.group());
            g.put("topic", c.topic());
            g.put("partition", c.partition());
            g.put("committed", c.offset());
            g.put("lag", Math.max(0, leo - 1 - c.offset()));
            groups.add(g);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("brokerId", props.brokerId());
        out.put("segmentBytes", props.segmentBytes());
        out.put("retentionMs", props.retentionMs());
        out.put("topics", logs.topics());
        out.put("partitions", partitions);
        out.put("groups", groups);
        return out;
    }
}
