package study.minimq.broker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import study.minimq.broker.raft.RaftNode;
import study.minimq.common.ClusterMetadata;
import study.minimq.common.FetchResponse;
import study.minimq.common.GroupProtocol;
import study.minimq.common.OffsetCommit;
import study.minimq.common.ProduceRequest;
import study.minimq.common.ProduceResponse;
import study.minimq.common.TopicInfo;

@RestController
public class BrokerApi {
    private final ReplicaManager replicas;
    private final OffsetStore offsets;
    private final Controller controller;
    private final RaftNode raft;
    private final Traffic traffic;
    private final GroupCoordinator groups;
    private static final String CLIENT = "X-MiniMq-Client";

    public BrokerApi(ReplicaManager replicas, OffsetStore offsets, Controller controller, RaftNode raft, Traffic traffic,
                     GroupCoordinator groups) {
        this.traffic = traffic;
        this.groups = groups;
        this.replicas = replicas;
        this.offsets = offsets;
        this.controller = controller;
        this.raft = raft;
    }

    // ---------------------------------------------------------------- 메타데이터

    /** 어느 브로커에 보내도 된다. 컨트롤러가 아니면 컨트롤러에게 넘긴다. */
    @PostMapping("/topics")
    public TopicInfo createTopic(@RequestBody TopicInfo req) {
        return controller.createTopic(req);
    }

    @GetMapping("/topics")
    public Collection<TopicInfo> topics() {
        return replicas.topics();
    }

    /** 생산자·소비자가 보는 "지도". 어느 브로커에 물어봐도 커밋된 메타데이터를 준다. */
    @GetMapping("/metadata")
    public ClusterMetadata metadata(@RequestHeader(value = CLIENT, required = false) String client) {
        ClusterMetadata m = raft.committed();
        traffic.record(client, "metadata", "", -1, 1, "v" + m.version());
        return m;
    }

    // ---------------------------------------------------------------- 생산자·소비자

    /** acks: 0, 1, -1(=all). 이 브로커가 그 파티션의 리더가 아니면 409. */
    @PostMapping("/produce")
    public CompletableFuture<ProduceResponse> produce(@RequestBody ProduceRequest req,
                                                      @RequestHeader(value = CLIENT, required = false) String client) {
        traffic.record(client, "produce", req.topic(), req.partition(), req.records().size(),
                req.acks() < 0 ? "all" : Integer.toString(req.acks()));
        return replicas.produce(req.topic(), req.partition(), req.acks(), req.records())
                .thenApply(r -> new ProduceResponse(req.topic(), req.partition(), r[0], r[1]));
    }

    /** 풀 + 롱 폴링 (그림 4.14). HW(합의 오프셋) 앞까지만 준다. */
    @GetMapping("/fetch")
    public CompletableFuture<FetchResponse> fetch(@RequestParam String topic,
                                                 @RequestParam int partition,
                                                 @RequestParam long offset,
                                                 @RequestParam(defaultValue = "500") int max,
                                                 @RequestParam(defaultValue = "0") long waitMs,
                                                 @RequestHeader(value = CLIENT, required = false) String client) {
        return replicas.consumerFetch(topic, partition, offset, max, waitMs).thenApply(r -> {
            traffic.record(client, "fetch", topic, partition, r.messages().size(), null);
            return r;
        });
    }

    /** 오프셋 커밋. 그룹 코디네이터(= __consumer_offsets 의 해당 파티션 리더)에게 보낸다. */
    @PostMapping("/offsets")
    public CompletableFuture<Void> commit(@RequestBody List<OffsetCommit> commits,
                                          @RequestParam(required = false) String memberId,
                                          @RequestParam(required = false) Integer generation,
                                          @RequestHeader(value = CLIENT, required = false) String client) {
        if (!commits.isEmpty()) groups.checkGeneration(commits.get(0).group(), memberId, generation);
        traffic.record(client, "commit", "", -1, 1, commits.isEmpty() ? null : commits.get(0).group());
        return offsets.commit(commits);
    }

    @GetMapping("/offsets")
    public Map<Integer, Long> committed(@RequestParam String group, @RequestParam String topic) {
        return offsets.committed(group, topic);
    }

    // ---------------------------------------------------------------- 소비자 그룹 (그림 4.15~4.20)

    @PostMapping("/group/join")
    public CompletableFuture<GroupProtocol.JoinResponse> join(@RequestBody GroupProtocol.JoinRequest req) {
        return groups.join(req);
    }

    @PostMapping("/group/sync")
    public CompletableFuture<GroupProtocol.SyncResponse> sync(@RequestBody GroupProtocol.SyncRequest req) {
        return groups.sync(req);
    }

    @PostMapping("/group/heartbeat")
    public GroupProtocol.HeartbeatResponse heartbeat(@RequestBody GroupProtocol.HeartbeatRequest req) {
        return groups.heartbeat(req);
    }

    @PostMapping("/group/leave")
    public void leave(@RequestBody GroupProtocol.LeaveRequest req) {
        groups.leave(req);
    }

    // ---------------------------------------------------------------- 브로커끼리

    /** 팔로워가 리더에게서 가져간다. offset = 팔로워의 LEO. */
    @GetMapping("/replica/fetch")
    public CompletableFuture<FetchResponse> replicaFetch(@RequestParam String topic,
                                                        @RequestParam int partition,
                                                        @RequestParam long offset,
                                                        @RequestParam int replicaId,
                                                        @RequestParam int leaderEpoch,
                                                        @RequestParam(defaultValue = "2000") int max,
                                                        @RequestParam(defaultValue = "500") long waitMs) {
        return replicas.replicaFetch(topic, partition, offset, replicaId, leaderEpoch, max, waitMs);
    }

    /** 팔로워가 잘라낼 위치를 묻는다: 그 epoch 는 리더 로그에서 어디까지인가. */
    @GetMapping("/replica/epoch-end")
    public long epochEnd(@RequestParam String topic, @RequestParam int partition, @RequestParam int epoch) {
        return replicas.epochEnd(topic, partition, epoch);
    }

    /** 파티션 리더가 컨트롤러에게 ISR 을 바꿔 달라고 한다. */
    @PostMapping("/controller/alter-isr")
    public void alterIsr(@RequestBody ReplicaManager.AlterIsr req) {
        controller.alterIsr(req);
    }
}
