package study.minimq.zk;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.Json;

/**
 * ZooKeeper-lite 의 본체. 경로("/brokers/1")마다 데이터를 갖는 노드 나무.
 *
 * <ul>
 *   <li><b>세션</b>: 클라이언트(브로커)가 열고 박동(ping)으로 유지한다. sessionTimeoutMs 동안 박동이 없으면 끝난다</li>
 *   <li><b>임시 노드</b>: 세션에 묶인 노드. 세션이 끝나면 저절로 지워진다 → 서비스 탐색(/brokers), 컨트롤러 선출(/controller)</li>
 *   <li><b>버전</b>: 노드를 고칠 때마다 1 오른다. "이 버전일 때만 고쳐라"(CAS) 로 동시에 고쳐도 덮어쓰지 않는다</li>
 *   <li><b>zxid</b>: 바뀔 때마다 1 오르는 전체 순번. 노드가 만들어진 순번(czxid)은 컨트롤러 epoch 로 쓴다</li>
 *   <li><b>감시(watch)</b>: "zxid 가 N 보다 커지면 알려 달라" 고 기다린다 (롱 폴링)</li>
 * </ul>
 * 1대뿐이라 복제는 없다. 영구 노드만 파일에 남기고, 임시 노드·세션은 재시작하면 사라진다 (진짜 주키퍼도 세션은 다시 맺어야 한다).
 */
public final class ZkStore {

    public record Node(String path, String data, long version, long czxid, long mzxid, Long ephemeralOwner) {
    }

    public record Session(long id, String name, long createdAt, long lastPing) {
    }

    public record Event(long at, long seq, long zxid, String kind, String path, String who, String text) {
    }

    /** 실패 이유: NODE_EXISTS, NO_NODE, BAD_VERSION, CHECK_FAILED, NO_SESSION */
    public static final class ZkException extends RuntimeException {
        public final String code;

        public ZkException(String code, String message) {
            super(code + ": " + message);
            this.code = code;
        }
    }

    private static final int MAX_EVENTS = 120;

    private final Path file;
    private final long sessionTimeoutMs;
    private final LongSupplier clock;
    private final TreeMap<String, Node> nodes = new TreeMap<>();
    private final Map<Long, Session> sessions = new LinkedHashMap<>();
    private final List<CompletableFuture<Long>> watchers = new ArrayList<>();
    private final Deque<Event> events = new ArrayDeque<>();
    private final AtomicLong sessionIds = new AtomicLong(System.currentTimeMillis() % 100_000 * 1000);
    private long zxid;
    private long eventSeq;

    public ZkStore(Path dataDir, long sessionTimeoutMs, LongSupplier clock) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.clock = clock;
        this.file = dataDir == null ? null : dataDir.resolve("zk-data.json");
        load();
        event("start", "/", "zookeeper-lite", "시작 (영구 노드 " + nodes.size() + "개, 세션은 모두 새로)");
    }

    // ------------------------------------------------------------------ 세션

    public synchronized Session openSession(String name) {
        long id = sessionIds.incrementAndGet();
        Session s = new Session(id, name, clock.getAsLong(), clock.getAsLong());
        sessions.put(id, s);
        event("session_open", "", name, name + " 세션 시작 (박동이 " + sessionTimeoutMs / 1000 + "초 끊기면 끝남)");
        return s;
    }

    public synchronized void ping(long id) {
        Session s = sessions.get(id);
        if (s == null) throw new ZkException("NO_SESSION", "세션 " + id + " 이 없다 (끝났다)");
        sessions.put(id, new Session(s.id(), s.name(), s.createdAt(), clock.getAsLong()));
    }

    public synchronized void closeSession(long id) {
        Session s = sessions.remove(id);
        if (s != null) dropEphemerals(s, "세션을 닫음");
    }

    /** 박동이 끊긴 세션을 끝내고 그 세션의 임시 노드를 지운다. */
    public synchronized void expireSessions() {
        long now = clock.getAsLong();
        for (Iterator<Session> it = sessions.values().iterator(); it.hasNext(); ) {
            Session s = it.next();
            if (now - s.lastPing() > sessionTimeoutMs) {
                it.remove();
                event("session_expired", "", s.name(), s.name() + " 세션 만료: 박동이 " + sessionTimeoutMs / 1000 + "초 넘게 없었다");
                dropEphemerals(s, "세션 만료");
            }
        }
    }

    private void dropEphemerals(Session s, String why) {
        List<String> mine = nodes.values().stream().filter(n -> Long.valueOf(s.id()).equals(n.ephemeralOwner())).map(Node::path).toList();
        for (String p : mine) {
            nodes.remove(p);
            zxid++;
            event("deleted", p, s.name(), "임시 노드 " + p + " 삭제 (" + s.name() + " " + why + ")");
        }
        if (!mine.isEmpty()) notifyWatchers();
    }

    // ------------------------------------------------------------------ 노드

    public synchronized Node create(String path, String data, Long ephemeralSession, String who) {
        if (nodes.containsKey(path)) {
            event("create_conflict", path, who, who + " 가 " + path + " 를 만들려 했지만 이미 있다 (" + shortData(nodes.get(path)) + ")");
            throw new ZkException("NODE_EXISTS", path);
        }
        if (ephemeralSession != null && !sessions.containsKey(ephemeralSession)) throw new ZkException("NO_SESSION", "세션이 없다");
        zxid++;
        Node n = new Node(path, data, 0, zxid, zxid, ephemeralSession);
        nodes.put(path, n);
        event("created", path, who, who + " 가 " + (ephemeralSession != null ? "임시 " : "") + "노드 " + path + " 생성 (czxid " + zxid + ")");
        persist();
        notifyWatchers();
        return n;
    }

    public synchronized Node get(String path) {
        Node n = nodes.get(path);
        if (n == null) throw new ZkException("NO_NODE", path);
        return n;
    }

    /**
     * expectedVersion 일 때만 고친다(-1 이면 상관없이). checkPath 가 있으면 그 노드의 czxid 가 checkCzxid 일 때만
     * (= "내가 아직 그 /controller 인가" 를 함께 확인. 낡은 컨트롤러의 쓰기를 막는다).
     */
    public synchronized Node set(String path, String data, long expectedVersion, String checkPath, Long checkCzxid, String who) {
        if (checkPath != null) {
            Node c = nodes.get(checkPath);
            if (c == null || c.czxid() != checkCzxid) {
                event("check_failed", path, who, who + " 의 " + path + " 쓰기 거부: " + checkPath + " 의 주인이 바뀌었다 (낡은 컨트롤러)");
                throw new ZkException("CHECK_FAILED", checkPath + " 이(가) 바뀌었다");
            }
        }
        Node n = nodes.get(path);
        if (n == null) throw new ZkException("NO_NODE", path);
        if (expectedVersion >= 0 && n.version() != expectedVersion) throw new ZkException("BAD_VERSION", "지금 버전 " + n.version());
        zxid++;
        Node next = new Node(path, data, n.version() + 1, n.czxid(), zxid, n.ephemeralOwner());
        nodes.put(path, next);
        event("set", path, who, who + " 가 " + path + " 고침 (버전 " + next.version() + ")");
        persist();
        notifyWatchers();
        return next;
    }

    public synchronized void delete(String path, String who) {
        if (nodes.remove(path) == null) throw new ZkException("NO_NODE", path);
        zxid++;
        event("deleted", path, who, who + " 가 " + path + " 삭제");
        persist();
        notifyWatchers();
    }

    public synchronized List<String> children(String path) {
        String prefix = path.endsWith("/") ? path : path + "/";
        List<String> out = new ArrayList<>();
        for (String p : nodes.subMap(prefix, prefix + Character.MAX_VALUE).keySet()) {
            String rest = p.substring(prefix.length());
            if (!rest.isEmpty() && !rest.contains("/")) out.add(rest);
        }
        return out;
    }

    // ------------------------------------------------------------------ 감시

    /** zxid 가 since 보다 커지면 완료 (그 사이 무엇이든 바뀌면 깨운다). */
    public synchronized CompletableFuture<Long> waitForChange(long since) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        if (zxid > since) f.complete(zxid);
        else {
            watchers.removeIf(CompletableFuture::isDone);
            watchers.add(f);
        }
        return f;
    }

    private void notifyWatchers() {
        long z = zxid;
        int n = 0;
        for (CompletableFuture<Long> w : watchers) if (w.complete(z)) n++;
        watchers.clear();
        if (n > 0) event("notify", "", "", "감시 알림: 바뀐 것을 기다리던 클라이언트 " + n + "곳에 알림 (zxid " + z + ")");
    }

    public synchronized long zxid() {
        return zxid;
    }

    // ------------------------------------------------------------------ 상태·기록

    public synchronized Map<String, Object> state() {
        long now = clock.getAsLong();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("zxid", zxid);
        m.put("sessionTimeoutMs", sessionTimeoutMs);
        List<Map<String, Object>> ss = new ArrayList<>();
        sessions.values().forEach(s -> ss.add(Map.of("id", s.id(), "name", s.name(), "lastPingAgoMs", now - s.lastPing())));
        m.put("sessions", ss);
        List<Map<String, Object>> ns = new ArrayList<>();
        for (Node n : nodes.values()) {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("path", n.path());
            x.put("version", n.version());
            x.put("czxid", n.czxid());
            x.put("ephemeral", n.ephemeralOwner() != null);
            x.put("owner", n.ephemeralOwner() == null ? null : sessions.containsKey(n.ephemeralOwner()) ? sessions.get(n.ephemeralOwner()).name() : null);
            x.put("data", n.data() != null && n.data().length() > 200 ? n.data().substring(0, 200) + "…" : n.data());
            ns.add(x);
        }
        m.put("nodes", ns);
        m.put("events", new ArrayList<>(events));
        return m;
    }

    private void event(String kind, String path, String who, String text) {
        events.addFirst(new Event(clock.getAsLong(), ++eventSeq, zxid, kind, path, who, text));
        while (events.size() > MAX_EVENTS) events.removeLast();
    }

    private static String shortData(Node n) {
        String d = n.data();
        return d == null ? "" : d.length() > 40 ? d.substring(0, 40) + "…" : d;
    }

    // ------------------------------------------------------------------ 디스크 (영구 노드만)

    private record Saved(long zxid, List<Node> nodes) {
    }

    private void persist() {
        if (file == null) return;
        try {
            Files.createDirectories(file.getParent());
            List<Node> persistent = nodes.values().stream().filter(n -> n.ephemeralOwner() == null).toList();
            Path tmp = file.resolveSibling("zk-data.json.tmp");
            Json.MAPPER.writeValue(tmp.toFile(), new Saved(zxid, persistent));
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void load() {
        if (file == null || !Files.exists(file)) return;
        try {
            Saved s = Json.MAPPER.readValue(file.toFile(), new TypeReference<Saved>() {});
            zxid = s.zxid();
            s.nodes().forEach(n -> nodes.put(n.path(), n));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
