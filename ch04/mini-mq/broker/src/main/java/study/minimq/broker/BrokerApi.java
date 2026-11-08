package study.minimq.broker;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import study.minimq.broker.raft.RaftNode;
import study.minimq.common.ClusterMetadata;
import study.minimq.common.FetchResponse;
import study.minimq.common.OffsetCommit;
import study.minimq.common.ProduceRequest;
import study.minimq.common.ProduceResponse;
import study.minimq.common.TopicInfo;

@RestController
public class BrokerApi {
    private final LogManager logs;
    private final OffsetStore offsets;
    private final Controller controller;
    private final RaftNode raft;

    public BrokerApi(LogManager logs, OffsetStore offsets, Controller controller, RaftNode raft) {
        this.logs = logs;
        this.offsets = offsets;
        this.controller = controller;
        this.raft = raft;
    }

    /** 어느 브로커에 보내도 된다. 컨트롤러가 아니면 컨트롤러에게 넘긴다. */
    @PostMapping("/topics")
    public TopicInfo createTopic(@RequestBody TopicInfo req) {
        return controller.createTopic(req);
    }

    @GetMapping("/topics")
    public Collection<TopicInfo> topics() {
        return logs.topics();
    }

    /** 생산자·소비자가 보는 "지도". 어느 브로커에 물어봐도 커밋된 메타데이터를 준다. */
    @GetMapping("/metadata")
    public ClusterMetadata metadata() {
        return raft.committed();
    }

    /** 생산자가 묶음을 보낸다. 이 브로커가 그 파티션의 리더가 아니면 409. */
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

    /** 오프셋은 그 파티션의 리더 브로커에 저장한다 (2단계 임시. 4단계에서 __consumer_offsets 로). */
    @PostMapping("/offsets")
    public void commit(@RequestBody OffsetCommit c) {
        logs.log(c.topic(), c.partition());
        offsets.commit(c);
    }

    @GetMapping("/offsets")
    public long committed(@RequestParam String group, @RequestParam String topic, @RequestParam int partition) {
        logs.log(topic, partition);
        return offsets.committed(group, topic, partition);
    }
}
