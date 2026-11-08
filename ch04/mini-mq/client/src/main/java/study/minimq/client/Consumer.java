package study.minimq.client;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.FetchResponse;
import study.minimq.common.GroupProtocol.HeartbeatRequest;
import study.minimq.common.GroupProtocol.HeartbeatResponse;
import study.minimq.common.GroupProtocol.JoinRequest;
import study.minimq.common.GroupProtocol.JoinResponse;
import study.minimq.common.GroupProtocol.LeaveRequest;
import study.minimq.common.GroupProtocol.SyncRequest;
import study.minimq.common.GroupProtocol.SyncResponse;
import study.minimq.common.Message;
import study.minimq.common.OffsetCommit;
import study.minimq.common.Topics;

/**
 * 소비자 클라이언트 (그림 4.14~4.20). 풀 모델 + 롱 폴링, 소비자 그룹.
 *
 * <p><b>그룹 모드</b>(partitions 를 비우면): 그룹 코디네이터(그룹 이름 해시로 정한 살아 있는 브로커)에 합류하고, 리더 소비자로 뽑히면 파티션 배치 계획을 만든다. 1초마다 박동을 보내고,
 * 응답이 "다시 합류하라"면 다음 poll 때 재조정에 들어간다. 재조정 동안은 소비하지 않는다.
 *
 * <p><b>정적 모드</b>(partitions 를 주면): 그룹 없이 그 파티션만 읽는다.
 *
 * <p>파티션은 리더 브로커에서 HW(합의 오프셋) 앞까지만 받는다. 오프셋은 코디네이터를 거쳐 조정 서비스(ZooKeeper-lite)에 커밋한다.
 * {@link #commit()} 을 처리 전에 부르면 최대 한 번, 처리 후에 부르면 최소 한 번.
 */
public final class Consumer implements AutoCloseable {

    /** @param assignor range | roundrobin (리더 소비자가 배치 계획을 만들 때) */
    public record Config(String bootstrap, String group, String topic, List<Integer> partitions,
                         int maxRecords, long waitMs, String assignor) {
    }

    /** 재조정이 끝나 새 배정을 받았을 때 알려 준다. */
    public interface AssignmentListener {
        void onAssigned(int generation, String memberId, boolean leader, List<Integer> partitions, Map<String, List<Integer>> plan);
    }

    private final Config cfg;
    private final String clientId;
    private final boolean grouped;
    private final ClusterView view;
    private final AssignmentListener listener;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    private volatile String memberId;
    private volatile int generation = -1;
    private volatile boolean needRejoin;
    private volatile boolean joining;
    private volatile boolean closed;
    private volatile boolean abandoned;
    private volatile String pendingCommand;
    private volatile List<Integer> assigned = List.of();

    private final Map<Integer, Long> position = new ConcurrentHashMap<>(); // 다음에 읽을 오프셋
    private final Map<Integer, Long> lastCommitted = new ConcurrentHashMap<>();
    private final Map<Integer, Long> logEnd = new ConcurrentHashMap<>();
    private final Set<Integer> unavailable = ConcurrentHashMap.newKeySet();
    private volatile boolean positionsLoaded;

    public Consumer(Config cfg, String clientId, AssignmentListener listener) throws IOException {
        this.cfg = cfg;
        this.clientId = clientId;
        this.listener = listener;
        this.view = new ClusterView(cfg.bootstrap());
        int n = view.partitions(cfg.topic());
        this.grouped = cfg.partitions() == null || cfg.partitions().isEmpty();
        if (grouped) {
            needRejoin = true;
            Thread.ofVirtual().name("heartbeat").start(this::heartbeatLoop);
        } else {
            assigned = cfg.partitions();
        }
        if (!grouped && assigned.stream().anyMatch(p -> p >= n)) throw new IOException("없는 파티션: " + assigned);
    }

    // ------------------------------------------------------------------ 그룹

    /** 재조정: join → (리더면 배치 계획) → sync. 코디네이터는 모두 다시 합류할 때까지 join 응답을 미룬다. */
    private void rejoin() throws IOException {
        joining = true;
        try {
            MiniMqHttp coord = coordinator();
            JoinResponse jr = coord.post("/group/join", new JoinRequest(cfg.group(), memberId, clientId, cfg.topic()),
                    JoinResponse.class, Duration.ofSeconds(20));
            memberId = jr.memberId();
            boolean leader = jr.memberId().equals(jr.leaderId());
            Map<String, List<Integer>> plan = leader ? assign(jr.members(), view.partitions(cfg.topic()), cfg.assignor()) : null;
            SyncResponse sr = coord.post("/group/sync", new SyncRequest(cfg.group(), jr.generation(), memberId, plan),
                    SyncResponse.class, Duration.ofSeconds(20));
            generation = sr.generation();
            assigned = sr.partitions().stream().sorted().toList();
            position.clear();
            lastCommitted.clear();
            logEnd.clear();
            unavailable.clear();
            positionsLoaded = false;
            needRejoin = false;
            if (listener != null) listener.onAssigned(generation, memberId, leader, assigned, plan);
        } catch (MiniMqHttp.HttpStatusException e) {
            if (e.getMessage().contains("UNKNOWN_MEMBER")) memberId = null;
            view.invalidate();
            throw e;
        } catch (IOException e) {
            view.invalidate();
            throw e;
        } finally {
            joining = false;
        }
    }

    /** 리더 소비자가 만드는 파티션 배치 계획. range = 연속 구간씩, roundrobin = 번갈아. 파티션보다 멤버가 많으면 남는 멤버는 논다. */
    static Map<String, List<Integer>> assign(List<String> members, int partitions, String assignor) {
        List<String> ms = members.stream().sorted().toList();
        Map<String, List<Integer>> plan = new TreeMap<>();
        ms.forEach(m -> plan.put(m, new ArrayList<>()));
        if ("roundrobin".equalsIgnoreCase(assignor)) {
            for (int p = 0; p < partitions; p++) plan.get(ms.get(p % ms.size())).add(p);
        } else {
            int base = partitions / ms.size(), extra = partitions % ms.size(), p = 0;
            for (int i = 0; i < ms.size(); i++) {
                int count = base + (i < extra ? 1 : 0);
                for (int k = 0; k < count; k++) plan.get(ms.get(i)).add(p++);
            }
        }
        return plan;
    }

    private void heartbeatLoop() {
        while (!closed) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                return;
            }
            if (closed || memberId == null || joining || needRejoin) continue;
            int sentGeneration = generation;
            try {
                HeartbeatResponse r = coordinator().post("/group/heartbeat",
                        new HeartbeatRequest(cfg.group(), sentGeneration, memberId), HeartbeatResponse.class, Duration.ofSeconds(2));
                if (r.command() != null) pendingCommand = r.command();
                if (r.rebalance() && generation == sentGeneration && !joining) needRejoin = true;
            } catch (MiniMqHttp.HttpStatusException e) {
                if (e.getMessage().contains("UNKNOWN_MEMBER")) memberId = null;
                view.invalidate();
                needRejoin = true;
            } catch (IOException e) {
                view.invalidate(); // 코디네이터가 옮겨 갔을 수 있다
            }
        }
    }

    private MiniMqHttp coordinator() throws IOException {
        var m = view.metadata();
        var b = m.brokers().get(Topics.coordinatorFor(cfg.group(), m));
        if (b == null) throw new IOException("그룹 코디네이터를 모름");
        return view.http(b.url());
    }

    // ------------------------------------------------------------------ 소비

    /** 맡은 파티션 모두에 동시에 롱 폴링을 걸고, 받은 것을 모아 돌려준다. 재조정이 필요하면 먼저 한다. */
    public List<Message> poll() throws IOException {
        if (needRejoin) rejoin();
        if (!positionsLoaded) loadPositions();
        List<Integer> parts = assigned;
        if (parts.isEmpty()) { // 파티션보다 멤버가 많아 노는 소비자
            sleep(cfg.waitMs());
            return List.of();
        }
        Map<Integer, Future<FetchResponse>> futures = new TreeMap<>();
        for (int p : parts) futures.put(p, pool.submit(() -> fetch(p)));

        List<Message> out = new ArrayList<>();
        IOException lastError = null;
        for (Map.Entry<Integer, Future<FetchResponse>> e : futures.entrySet()) {
            int p = e.getKey();
            FetchResponse res;
            try {
                res = e.getValue().get();
                unavailable.remove(p);
            } catch (Exception ex) {
                unavailable.add(p);
                view.invalidate();
                lastError = new IOException("파티션 " + p + " 을 못 읽음", ex.getCause() != null ? ex.getCause() : ex);
                continue;
            }
            logEnd.put(p, res.logEndOffset());
            long pos = position.get(p);
            if (pos < res.logStartOffset()) pos = res.logStartOffset(); // 보관 기간이 지나 지워진 구간은 건너뜀
            if (!res.messages().isEmpty()) pos = res.messages().get(res.messages().size() - 1).offset() + 1;
            position.put(p, pos);
            out.addAll(res.messages());
        }
        if (unavailable.size() == parts.size() && lastError != null) throw lastError;
        return out;
    }

    /** 코디네이터에게 이 그룹이 마지막으로 소비한 오프셋을 받아 그 다음부터 읽는다. 커밋이 없으면 0 부터. */
    private void loadPositions() throws IOException {
        Map<Integer, Long> committed;
        try {
            committed = coordinator().get("/offsets?group=%s&topic=%s".formatted(enc(cfg.group()), enc(cfg.topic())),
                    new TypeReference<Map<Integer, Long>>() {}, Duration.ofSeconds(2));
        } catch (IOException e) {
            view.invalidate();
            throw new IOException("코디네이터에게 오프셋을 못 받음", e);
        }
        for (int p : assigned) {
            long c = committed.getOrDefault(p, -1L);
            position.put(p, c + 1);
            lastCommitted.put(p, c);
        }
        positionsLoaded = true;
    }

    private FetchResponse fetch(int p) throws IOException {
        String path = "/fetch?topic=%s&partition=%d&offset=%d&max=%d&waitMs=%d"
                .formatted(enc(cfg.topic()), p, position.get(p), cfg.maxRecords(), cfg.waitMs());
        return view.leader(cfg.topic(), p).get(path, new TypeReference<FetchResponse>() {}, Duration.ofMillis(cfg.waitMs() + 5_000));
    }

    /**
     * 지금까지 poll 로 받은 위치까지 커밋한다. 바뀐 파티션만 한 번에 보낸다.
     * 그룹 모드면 세대를 함께 보낸다. 그 사이 재조정이 끝나 세대가 바뀌었으면 거절되고(ILLEGAL_GENERATION) 다시 합류한다.
     */
    public boolean commit() {
        List<OffsetCommit> changed = new ArrayList<>();
        for (int p : assigned) {
            Long pos = position.get(p);
            if (pos != null && pos - 1 != lastCommitted.getOrDefault(p, -1L)) {
                changed.add(new OffsetCommit(cfg.group(), cfg.topic(), p, pos - 1));
            }
        }
        if (changed.isEmpty()) return true;
        String query = grouped && memberId != null ? "?memberId=%s&generation=%d".formatted(enc(memberId), generation) : "";
        try {
            coordinator().post("/offsets" + query, changed, Void.class);
            changed.forEach(c -> lastCommitted.put(c.partition(), c.offset()));
            return true;
        } catch (MiniMqHttp.HttpStatusException e) {
            if (e.getMessage().contains("UNKNOWN_MEMBER")) memberId = null;
            if (grouped && (e.getMessage().contains("GENERATION") || e.getMessage().contains("UNKNOWN_MEMBER"))) needRejoin = true;
            view.invalidate();
            return false;
        } catch (IOException e) {
            view.invalidate();
            return false;
        }
    }

    // ------------------------------------------------------------------ 상태

    public Map<Integer, Long> lag() {
        Map<Integer, Long> out = new TreeMap<>();
        for (int p : assigned) {
            Long pos = position.get(p);
            Long end = logEnd.get(p);
            if (pos != null && end != null) out.put(p, Math.max(0, end - pos));
        }
        return out;
    }

    public Set<Integer> unavailable() {
        return new TreeSet<>(unavailable);
    }

    public List<Integer> assigned() {
        return assigned;
    }

    public int generation() {
        return generation;
    }

    /** 대시보드에서 온 실험 지시 (crash, leave). 한 번 꺼내면 지워진다. */
    public String takeCommand() {
        String c = pendingCommand;
        pendingCommand = null;
        return c;
    }

    /** 장애 흉내: 그룹에 알리지 않고 멈춘다. 코디네이터는 박동이 끊긴 뒤에야 안다. */
    public void abandon() {
        abandoned = true;
        close();
    }

    /** 정상 종료: 그룹에서 나간다고 알린다 → 남은 멤버로 바로 재조정. */
    @Override
    public void close() {
        closed = true;
        if (grouped && !abandoned && memberId != null) {
            try {
                coordinator().post("/group/leave", new LeaveRequest(cfg.group(), memberId), Void.class, Duration.ofSeconds(2));
            } catch (IOException ignored) {
            }
        }
        pool.shutdownNow();
    }

    private static void sleep(long ms) throws IOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
