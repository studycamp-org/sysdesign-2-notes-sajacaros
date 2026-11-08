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

/** 대시보드용. 고립 실험 중에도 열려 있다. */
@RestController
public class AdminApi {
    private final BrokerProperties props;
    private final RaftNode raft;
    private final ReplicaManager replicas;
    private final OffsetStore offsets;
    private final Traffic traffic;
    private final GroupCoordinator groups;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public AdminApi(BrokerProperties props, RaftNode raft, ReplicaManager replicas, OffsetStore offsets, Traffic traffic,
                    GroupCoordinator groups) {
        this.traffic = traffic;
        this.groups = groups;
        this.props = props;
        this.raft = raft;
        this.replicas = replicas;
        this.offsets = offsets;
    }

    /** 이 브로커 하나의 상태: Raft 상태 + 가진 사본들 + 코디네이터로서 맡은 그룹 오프셋. */
    @GetMapping("/admin/node")
    public Map<String, Object> node() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("raft", raft.state());
        out.put("replicationDelayMs", replicas.replicationDelayMs());
        out.put("replicas", replicas.states());
        out.put("offsets", offsets.all());
        out.put("traffic", traffic.snapshot());
        out.put("groups", groups.states());
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

    /** 팔로워로서 가져가기 전에 쉰다 → 뒤처진다 → ISR 에서 빠지거나, ACK=all 이 느려진다. */
    @PostMapping("/admin/slow")
    public Map<String, Object> slow(@RequestParam long ms) {
        replicas.setReplicationDelayMs(ms);
        return Map.of("id", props.brokerId(), "replicationDelayMs", ms);
    }

    /**
     * 프로세스를 바로 죽인다. 컨테이너는 곧바로 다시 띄우지만 downSec 동안은 일어나지 않고 기다린다
     * (BrokerApplication.main). 컨트롤러가 죽음을 알아채고 리더를 옮기는 걸 보려고.
     */
    @PostMapping("/admin/crash")
    public Map<String, Object> crash(@RequestParam(defaultValue = "15") int downSec) throws IOException {
        java.nio.file.Files.writeString(java.nio.file.Path.of(props.dataDir()).resolve(BrokerApplication.DOWN_UNTIL),
                Long.toString(System.currentTimeMillis() + downSec * 1000L));
        raft.note("💥 강제 종료 (" + downSec + "초 동안 꺼져 있음)");
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            Runtime.getRuntime().halt(1);
        });
        return Map.of("id", props.brokerId(), "crash", true);
    }

    /** 이 브로커가 그 그룹의 코디네이터면 멤버에게 지시를 건다 (다음 박동 응답으로 전달). */
    @PostMapping("/admin/group-command")
    public Map<String, Object> groupCommand(@RequestParam String group, @RequestParam String member, @RequestParam String cmd) {
        return Map.of("delivered", groups.command(group, member, cmd));
    }

    /** 대시보드: 코디네이터가 누군지 몰라도 되게 모든 브로커에 뿌린다. */
    @PostMapping("/admin/cluster/group-command")
    public Map<String, Object> groupCommandAll(@RequestParam String group, @RequestParam String member, @RequestParam String cmd) {
        boolean delivered = false;
        for (String url : raft.members().values()) {
            try {
                String q = "/admin/group-command?group=%s&member=%s&cmd=%s".formatted(enc(group), enc(member), enc(cmd));
                Map<?, ?> r = new MiniMqHttp(url).post(q, Map.of(), Map.class, Duration.ofSeconds(2));
                delivered |= Boolean.TRUE.equals(r.get("delivered"));
            } catch (IOException ignored) {
            }
        }
        return Map.of("delivered", delivered);
    }

    private static String enc(String s) {
        return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
    }

    /** 대시보드가 다른 브로커에 실험 명령을 보낼 때: action = isolate | slow | crash. */
    @PostMapping("/admin/cluster/{action}")
    public Object forward(@org.springframework.web.bind.annotation.PathVariable String action,
                          @RequestParam int id, @RequestParam(required = false) String value) throws IOException {
        String query = switch (action) {
            case "isolate" -> "/admin/isolate?on=" + value;
            case "slow" -> "/admin/slow?ms=" + value;
            case "crash" -> "/admin/crash";
            default -> throw new IllegalArgumentException(action);
        };
        String url = raft.members().get(id);
        return new MiniMqHttp(url).post(query, Map.of(), Map.class, Duration.ofSeconds(2));
    }
}
