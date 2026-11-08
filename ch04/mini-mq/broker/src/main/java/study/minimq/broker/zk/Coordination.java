package study.minimq.broker.zk;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import study.minimq.common.ClusterMetadata;
import study.minimq.common.Json;

/**
 * 브로커가 조정 서비스(ZooKeeper-lite)와 하는 일 (교재 그림 4.8, 4.22).
 *
 * <ul>
 *   <li><b>서비스 탐색</b>: 세션을 열고 1초마다 박동. 임시 노드 /brokers/{id} 를 만든다. 박동이 끊기면 노드가 사라진다</li>
 *   <li><b>컨트롤러 선출</b>: /controller 가 비어 있으면 임시 노드로 만들어 본다. 먼저 만든 브로커가 컨트롤러.
 *       그 노드가 만들어진 순번(czxid)이 컨트롤러 epoch 다</li>
 *   <li><b>메타데이터</b>: /metadata 에 둔다. 컨트롤러만 고치고(버전 비교 + "/controller 가 아직 나인가" 확인),
 *       모든 브로커는 감시(watch)로 바뀐 걸 알아채 다시 읽는다</li>
 * </ul>
 * 브로커끼리는 투표하지 않는다. 조정 서비스가 1대라 그게 죽으면 선출과 메타데이터 변경이 멈춘다(단일 장애점).
 */
public final class Coordination {
    private static final Logger log = LoggerFactory.getLogger("events");
    private static final int MAX_EVENTS = 40;
    private static final String CONTROLLER = "/controller", METADATA = "/metadata", BROKERS = "/brokers";

    public record CoordEvent(long at, long seq, int node, String kind, String text) {
    }

    public record NodeState(int id, boolean controller, Integer controllerId, long epoch, Long session,
                            long metadataVersion, boolean isolated, List<CoordEvent> events) {
    }

    private final int id;
    private final String myUrl;
    private final Map<Integer, String> members;
    private final ZkClient zk;
    private final List<Consumer<ClusterMetadata>> listeners = new CopyOnWriteArrayList<>();
    private final ExecutorService writer = Executors.newSingleThreadExecutor(r -> new Thread(r, "zk-writer"));
    private final ExecutorService notifier = Executors.newSingleThreadExecutor(r -> new Thread(r, "zk-notify"));
    private final Deque<CoordEvent> events = new ArrayDeque<>();
    private long eventSeq;

    private volatile ClusterMetadata committed;
    private volatile long metadataZkVersion = -1;
    private volatile Integer controllerId;
    private volatile long controllerEpoch;
    private volatile boolean controller;
    private volatile long controllerSince;
    private volatile Set<Integer> live = Set.of();
    private volatile Long session;
    private volatile boolean isolated;
    private volatile long zxid;

    public Coordination(int id, String myUrl, Map<Integer, String> members, String zkUrl) {
        this.id = id;
        this.myUrl = myUrl;
        this.members = new TreeMap<>(members);
        this.zk = new ZkClient(zkUrl, "broker-" + id);
        this.committed = ClusterMetadata.initial(members);
    }

    public void start() {
        Thread.ofPlatform().name("zk-ping").daemon().start(this::pingLoop);
        Thread.ofPlatform().name("zk-watch").daemon().start(this::watchLoop);
    }

    // ------------------------------------------------------------------ 세션과 박동

    private void pingLoop() {
        while (true) {
            sleep(1000);
            Long s = session;
            if (s == null || isolated) continue; // 고립: 박동이 끊긴다 → 세션 만료 → 임시 노드가 지워진다
            try {
                zk.ping(s);
            } catch (ZkClient.ZkError e) {
                if ("NO_SESSION".equals(e.code)) sessionLost();
            } catch (IOException ignored) {
                // 조정 서비스에 닿지 않는다. 계속 시도
            }
        }
    }

    private synchronized void sessionLost() {
        if (session == null) return;
        session = null;
        boolean was = controller;
        controller = false;
        event(was ? "controller_lost" : "session_lost", "zookeeper-lite 세션이 끝났다" + (was ? " → 컨트롤러 자리도 잃음" : "") + ". 다시 세션을 열고 등록한다");
    }

    private void ensureSession() throws IOException {
        if (session != null) return;
        ZkClient.Session s = zk.openSession();
        try {
            zk.create(BROKERS, null, null);
        } catch (ZkClient.ZkError ignored) {
            // 이미 있다
        }
        try {
            zk.create(BROKERS + "/" + id, myUrl, s.id());
        } catch (ZkClient.ZkError e) {
            // 예전 세션의 노드가 아직 남아 있다(만료 전). 이 세션은 버리고 다음에 다시
            return;
        }
        session = s.id();
        event("registered", "zookeeper-lite 세션 " + s.id() + " 시작, 임시 노드 /brokers/" + id + " 등록 (서비스 탐색)");
    }

    // ------------------------------------------------------------------ 감시와 동기화

    private void watchLoop() {
        while (true) {
            try {
                if (isolated) {
                    sleep(200);
                    continue;
                }
                ensureSession();
                sync();
                zxid = zk.waitForChange(zxid, 1000); // 무엇이든 바뀌면 깨어난다
            } catch (IOException e) {
                sleep(500);
            } catch (RuntimeException e) {
                log.warn("zk 동기화 실패", e);
                sleep(500);
            }
        }
    }

    private void sync() throws IOException {
        Set<Integer> l = new TreeSet<>();
        for (String c : zk.children(BROKERS)) l.add(Integer.parseInt(c));
        live = l;

        Integer cid = null;
        long epoch = 0;
        try {
            ZkClient.Node c = zk.get(CONTROLLER);
            cid = Integer.parseInt(c.data().trim());
            epoch = c.czxid();
        } catch (ZkClient.ZkError e) {
            if (!"NO_NODE".equals(e.code)) throw e;
        }
        if (cid == null) {
            tryBecomeController();
            return;
        }
        boolean me = cid == id && session != null;
        if (controller && !me) event("controller_lost", "/controller 의 주인이 broker-" + cid + " 로 바뀌었다 → 컨트롤러에서 내려옴");
        if (!controller && me) controllerSince = System.currentTimeMillis();
        controller = me;
        if (!java.util.Objects.equals(controllerId, cid)) {
            if (!me) event("controller_seen", "감시 알림으로 새 컨트롤러를 앎: broker-" + cid + " (epoch " + epoch + ")");
            controllerId = cid;
        }
        controllerEpoch = epoch;

        try {
            ZkClient.Node m = zk.get(METADATA);
            if (m.version() != metadataZkVersion) {
                ClusterMetadata md = Json.MAPPER.readValue(m.data(), ClusterMetadata.class);
                metadataZkVersion = m.version();
                apply(md);
            }
        } catch (ZkClient.ZkError e) {
            if (!"NO_NODE".equals(e.code)) throw e;
        }
    }

    /** /controller 가 비어 있다: 임시 노드로 만들어 본다. 먼저 만든 쪽이 컨트롤러. */
    private void tryBecomeController() throws IOException {
        Long s = session;
        if (s == null || isolated) return;
        try {
            ZkClient.Node c = zk.create(CONTROLLER, Integer.toString(id), s);
            controller = true;
            controllerId = id;
            controllerEpoch = c.czxid();
            controllerSince = System.currentTimeMillis();
            event("controller_won", "/controller 를 먼저 만들었다 → 컨트롤러 (epoch " + c.czxid() + ")");
            try {
                zk.create(METADATA, Json.MAPPER.writeValueAsString(ClusterMetadata.initial(members)), null);
            } catch (ZkClient.ZkError ignored) {
                // 이미 있다
            }
            propose(md -> md.withController(id, controllerEpoch));
        } catch (ZkClient.ZkError e) {
            if ("NODE_EXISTS".equals(e.code)) event("controller_race_lost", "/controller 를 만들려 했지만 이미 있다 → 다른 브로커가 먼저 만들었다");
            else throw e;
        }
    }

    private void apply(ClusterMetadata md) {
        committed = md;
        ClusterMetadata snap = md;
        notifier.execute(() -> listeners.forEach(l -> l.accept(snap)));
    }

    // ------------------------------------------------------------------ 메타데이터 고치기 (컨트롤러만)

    /**
     * 메타데이터를 고친다: 최신 /metadata 를 읽어 바꾸고, 그 버전일 때만 + /controller 가 아직 나일 때만 쓴다.
     * 다른 쓰기와 겹치면 다시 읽어 다시 한다. 순서대로 하나씩 처리한다.
     */
    public CompletableFuture<ClusterMetadata> propose(UnaryOperator<ClusterMetadata> change) {
        if (!controller) return CompletableFuture.failedFuture(new NotControllerException(controllerId));
        CompletableFuture<ClusterMetadata> f = new CompletableFuture<>();
        writer.execute(() -> {
            for (int attempt = 0; attempt < 10; attempt++) {
                if (!controller || isolated) {
                    f.completeExceptionally(new NotControllerException(controllerId));
                    return;
                }
                try {
                    ZkClient.Node cur = zk.get(METADATA);
                    ClusterMetadata md = Json.MAPPER.readValue(cur.data(), ClusterMetadata.class);
                    ClusterMetadata changed = change.apply(md);
                    if (changed.equals(md)) {
                        f.complete(md);
                        return;
                    }
                    ClusterMetadata next = changed.withVersion(cur.version() + 1, controllerEpoch);
                    zk.set(METADATA, Json.MAPPER.writeValueAsString(next), cur.version(), CONTROLLER, controllerEpoch);
                    metadataZkVersion = cur.version() + 1;
                    apply(next);
                    f.complete(next);
                    return;
                } catch (ZkClient.ZkError e) {
                    if ("CHECK_FAILED".equals(e.code)) {
                        controller = false;
                        event("controller_lost", "메타데이터를 쓰려 했지만 /controller 가 바뀌어 거절됐다 (낡은 컨트롤러 펜싱)");
                        f.completeExceptionally(new NotControllerException(controllerId));
                        return;
                    }
                    // BAD_VERSION 등: 다시 읽고 다시
                } catch (IOException e) {
                    sleep(200);
                }
            }
            f.completeExceptionally(new IllegalStateException("메타데이터를 쓰지 못함"));
        });
        return f;
    }

    // ------------------------------------------------------------------ 바깥에서 보는 것

    public void addListener(Consumer<ClusterMetadata> l) {
        listeners.add(l);
        ClusterMetadata now = committed;
        notifier.execute(() -> l.accept(now));
    }

    public ClusterMetadata committed() {
        return committed;
    }

    /** 예전 Raft-lite 와 같은 이름. 조정 서비스에선 받은 것 = 커밋된 것. */
    public ClusterMetadata accepted() {
        return committed;
    }

    public boolean isLeader() {
        return controller;
    }

    public Integer leaderId() {
        return controllerId;
    }

    public String leaderUrl() {
        Integer c = controllerId;
        return c == null ? null : members.get(c);
    }

    /** 조정 서비스에 등록돼 있는(세션이 살아 있는) 브로커. */
    public Set<Integer> liveBrokers() {
        return live;
    }

    /** 컨트롤러가 된 뒤 지난 시간. 막 된 컨트롤러는 다른 브로커가 아직 등록 전일 수 있다. */
    public long controllerAgeMs() {
        return controller ? System.currentTimeMillis() - controllerSince : 0;
    }

    public int id() {
        return id;
    }

    public Map<Integer, String> members() {
        return members;
    }

    public void setIsolated(boolean on) {
        isolated = on;
        note(on ? "🔌 고립 시작: 조정 서비스에도 박동을 못 보낸다 → 곧 세션 만료" : "🔗 고립 해제: 다시 연결됨");
    }

    public boolean isolated() {
        return isolated;
    }

    public void note(String text) {
        event("note", text);
    }

    public synchronized NodeState state() {
        return new NodeState(id, controller, controllerId, controllerEpoch, session, committed.version(), isolated, new ArrayList<>(events));
    }

    /** 대시보드: 조정 서비스의 상태 (노드·세션·기록). */
    public Map<String, Object> zkState() throws IOException {
        return zk.state();
    }

    private synchronized void event(String kind, String text) {
        events.addFirst(new CoordEvent(System.currentTimeMillis(), ++eventSeq, id, kind, text));
        while (events.size() > MAX_EVENTS) events.removeLast();
        log.info("[broker-{}] {}", id, text);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
