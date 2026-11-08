package study.minimq.broker;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.broker.raft.RaftNode;
import study.minimq.client.MiniMqHttp;
import study.minimq.common.OffsetCommit;

/** 대시보드용. 고립 실험 중에도 열려 있다. */
@RestController
public class AdminApi {
    private final BrokerProperties props;
    private final RaftNode raft;
    private final LogManager logs;
    private final OffsetStore offsets;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public AdminApi(BrokerProperties props, RaftNode raft, LogManager logs, OffsetStore offsets) {
        this.props = props;
        this.raft = raft;
        this.logs = logs;
        this.offsets = offsets;
    }

    /** 이 브로커 하나의 상태: Raft 상태 + 들고 있는 파티션 + 그 파티션의 그룹 오프셋. */
    @GetMapping("/admin/node")
    public Map<String, Object> node() {
        List<Map<String, Object>> groups = new ArrayList<>();
        for (OffsetCommit c : offsets.all()) {
            if (!logs.isLeader(c.topic(), c.partition())) continue;
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
        out.put("raft", raft.state());
        out.put("partitions", logs.states());
        out.put("groups", groups);
        return out;
    }

    /** 클러스터 전체: 모든 브로커에 /admin/node 를 물어 모은다. 응답 없는 브로커는 reachable=false. */
    @GetMapping("/admin/cluster")
    public Map<String, Object> cluster() {
        Map<Integer, CompletableFuture<Map<String, Object>>> futures = new LinkedHashMap<>();
        raft.members().forEach((id, url) -> futures.put(id, CompletableFuture.supplyAsync(() -> {
            if (id == props.brokerId()) return node();
            try {
                return new MiniMqHttp(url).get("/admin/node", new TypeReference<Map<String, Object>>() {}, Duration.ofMillis(700));
            } catch (IOException e) {
                return null;
            }
        }, pool)));
        List<Map<String, Object>> nodes = new ArrayList<>();
        futures.forEach((id, f) -> {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("id", id);
            Map<String, Object> body = f.join();
            n.put("reachable", body != null);
            if (body != null) n.putAll(body);
            nodes.add(n);
        });
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("servedBy", props.brokerId());
        out.put("metadata", raft.committed());
        out.put("nodes", nodes);
        return out;
    }

    @PostMapping("/admin/isolate")
    public Map<String, Object> isolate(@RequestParam boolean on) {
        raft.setIsolated(on);
        return Map.of("id", props.brokerId(), "isolated", on);
    }

    /** 대시보드가 다른 브로커를 고립시키거나 풀 때. */
    @PostMapping("/admin/cluster/isolate")
    public Object isolateOther(@RequestParam int id, @RequestParam boolean on) throws IOException {
        if (id == props.brokerId()) return isolate(on);
        String url = raft.members().get(id);
        return new MiniMqHttp(url).post("/admin/isolate?on=" + on, Map.of(), Map.class, Duration.ofSeconds(2));
    }
}
