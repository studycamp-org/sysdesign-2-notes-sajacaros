package study.minimq.broker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import study.minimq.broker.raft.RaftNode;
import study.minimq.common.GroupProtocol.HeartbeatRequest;
import study.minimq.common.GroupProtocol.HeartbeatResponse;
import study.minimq.common.GroupProtocol.JoinRequest;
import study.minimq.common.GroupProtocol.JoinResponse;
import study.minimq.common.GroupProtocol.LeaveRequest;
import study.minimq.common.GroupProtocol.SyncRequest;
import study.minimq.common.GroupProtocol.SyncResponse;
import study.minimq.common.Topics;

/**
 * 소비자 그룹 코디네이터 (그림 4.15~4.20). 이 브로커가 그룹의 __consumer_offsets 파티션 리더일 때만 받는다.
 *
 * <pre>
 *   EMPTY ──join──▶ 재조정 준비 ──모두 합류 또는 시간 끝──▶ 배정 대기 ──리더가 계획 보냄──▶ 안정
 *                      ▲                                                                   │
 *                      └──── 합류 · 나가기 · 박동 끊김 ────────────────────────────────────┘
 * </pre>
 *
 * 재조정 중에는 그룹 전체가 소비를 멈춘다(교재: "재조정 동안은 소비가 멈춘다").
 * 그룹 상태는 메모리에만 둔다. 코디네이터가 바뀌면 멤버들이 새 코디네이터에 처음부터 다시 합류한다.
 */
@Component
public class GroupCoordinator {

    public enum State {EMPTY, PREPARING_REBALANCE, AWAITING_SYNC, STABLE}

    static final class Member {
        final String id;
        final String clientId;
        long lastHeartbeat;
        boolean joined;
        CompletableFuture<JoinResponse> join;
        CompletableFuture<SyncResponse> sync;
        List<Integer> assigned = List.of();
        String command;

        Member(String id, String clientId) {
            this.id = id;
            this.clientId = clientId;
        }
    }

    static final class Group {
        final String name;
        String topic;
        State state = State.EMPTY;
        int generation;
        String leader;
        long deadline;
        final Map<String, Member> members = new LinkedHashMap<>();

        Group(String name) {
            this.name = name;
        }
    }

    private final BrokerProperties props;
    private final RaftNode raft;
    private final ReplicaManager replicas;
    private final Map<String, Group> groups = new ConcurrentHashMap<>();

    public GroupCoordinator(BrokerProperties props, RaftNode raft, ReplicaManager replicas) {
        this.props = props;
        this.raft = raft;
        this.replicas = replicas;
    }

    private boolean isCoordinator(String group) {
        return replicas.isLeader(Topics.CONSUMER_OFFSETS, Topics.offsetsPartition(group));
    }

    private Group group(String name) {
        if (!isCoordinator(name)) {
            groups.remove(name);
            throw new ResponseStatusException(HttpStatus.CONFLICT, "NOT_COORDINATOR: 그룹 " + name + " 의 코디네이터가 아님");
        }
        return groups.computeIfAbsent(name, Group::new);
    }

    // ------------------------------------------------------------------ join

    public CompletableFuture<JoinResponse> join(JoinRequest r) {
        Group g = group(r.group());
        synchronized (g) {
            g.topic = r.topic();
            Member m = r.memberId() == null ? null : g.members.get(r.memberId());
            boolean isNew = m == null;
            if (isNew) {
                String id = r.clientId() + "#" + Integer.toHexString(ThreadLocalRandom.current().nextInt(0x1000, 0x10000));
                m = new Member(id, r.clientId());
                g.members.put(id, m);
            }
            if (g.state != State.PREPARING_REBALANCE) {
                prepare(g, isNew ? short_(m) + " 합류" : short_(m) + " 다시 합류 요청",
                        g.state == State.EMPTY ? props.initialRebalanceDelayMs() : props.rebalanceTimeoutMs());
            }
            m.lastHeartbeat = now();
            m.joined = true;
            if (m.join != null && !m.join.isDone()) m.join.completeExceptionally(conflict("REBALANCE_IN_PROGRESS"));
            m.join = new CompletableFuture<>();
            CompletableFuture<JoinResponse> f = m.join;
            maybeCompleteJoin(g);
            return f;
        }
    }

    /** 재조정 시작: 모두 다시 합류해야 한다. 기존 멤버는 다음 박동 응답에서 이걸 안다. */
    private void prepare(Group g, String reason, long waitMs) {
        g.state = State.PREPARING_REBALANCE;
        g.deadline = now() + waitMs;
        for (Member m : g.members.values()) {
            m.joined = false;
            if (m.sync != null && !m.sync.isDone()) m.sync.completeExceptionally(conflict("REBALANCE_IN_PROGRESS"));
        }
        raft.note("👥 " + g.name + ": " + reason + " → 재조정 시작 (모두 다시 합류해야 함)");
    }

    private void maybeCompleteJoin(Group g) {
        if (g.state != State.PREPARING_REBALANCE) return;
        boolean allJoined = g.members.values().stream().allMatch(m -> m.joined);
        boolean firstRound = g.generation == 0;
        if ((allJoined && !firstRound) || now() >= g.deadline) completeJoin(g);
    }

    private void completeJoin(Group g) {
        for (Iterator<Member> it = g.members.values().iterator(); it.hasNext(); ) {
            Member m = it.next();
            if (!m.joined) {
                it.remove();
                raft.note("👥 " + g.name + ": " + short_(m) + " 이 제때 다시 합류하지 않아 제외");
            }
        }
        if (g.members.isEmpty()) {
            g.state = State.EMPTY;
            return;
        }
        g.generation++;
        if (g.leader == null || !g.members.containsKey(g.leader)) g.leader = g.members.keySet().iterator().next();
        g.state = State.AWAITING_SYNC;
        g.deadline = now() + props.rebalanceTimeoutMs();
        List<String> ids = new ArrayList<>(g.members.keySet());
        for (Member m : g.members.values()) {
            m.assigned = List.of();
            m.join.complete(new JoinResponse(m.id, g.generation, g.leader, m.id.equals(g.leader) ? ids : List.of(), g.topic));
        }
        raft.note("👥 " + g.name + ": 세대 " + g.generation + " 멤버 " + ids.size() + "명, 리더 소비자 "
                + short_(g.members.get(g.leader)) + " → 배치 계획을 기다림");
    }

    // ------------------------------------------------------------------ sync

    public CompletableFuture<SyncResponse> sync(SyncRequest r) {
        Group g = group(r.group());
        synchronized (g) {
            Member m = member(g, r.memberId());
            if (r.generation() != g.generation) throw conflict("ILLEGAL_GENERATION");
            m.lastHeartbeat = now();
            if (g.state == State.AWAITING_SYNC && m.id.equals(g.leader) && r.assignments() != null) {
                Map<String, List<Integer>> plan = new TreeMap<>();
                for (Member x : g.members.values()) {
                    x.assigned = r.assignments().getOrDefault(x.id, List.of());
                    plan.put(short_(x), x.assigned);
                }
                g.state = State.STABLE;
                for (Member x : g.members.values()) {
                    if (x.sync != null && !x.sync.isDone()) x.sync.complete(new SyncResponse(g.generation, x.assigned));
                }
                raft.note("👥 " + g.name + ": 세대 " + g.generation + " 배정 " + plan + " → 소비 재개");
            }
            if (g.state == State.STABLE) return CompletableFuture.completedFuture(new SyncResponse(g.generation, m.assigned));
            if (m.sync != null && !m.sync.isDone()) m.sync.completeExceptionally(conflict("REBALANCE_IN_PROGRESS"));
            m.sync = new CompletableFuture<>();
            return m.sync;
        }
    }

    // ------------------------------------------------------------------ heartbeat · leave

    public HeartbeatResponse heartbeat(HeartbeatRequest r) {
        Group g = group(r.group());
        synchronized (g) {
            Member m = member(g, r.memberId());
            m.lastHeartbeat = now();
            String cmd = m.command;
            m.command = null;
            boolean rebalance = g.state == State.PREPARING_REBALANCE || r.generation() != g.generation;
            return new HeartbeatResponse(rebalance, cmd);
        }
    }

    public void leave(LeaveRequest r) {
        Group g = group(r.group());
        synchronized (g) {
            Member m = g.members.remove(r.memberId());
            if (m == null) return;
            if (g.members.isEmpty()) {
                g.state = State.EMPTY;
                raft.note("👥 " + g.name + ": " + short_(m) + " 나감 → 그룹이 비었음");
            } else {
                prepare(g, short_(m) + " 나감", props.rebalanceTimeoutMs());
                maybeCompleteJoin(g);
            }
        }
    }

    /** 0.2초마다: 박동이 끊긴 멤버 정리, 재조정 시간 초과 처리. */
    @Scheduled(fixedDelay = 200)
    void tick() {
        long now = now();
        for (Group g : groups.values()) {
            if (!isCoordinator(g.name)) {
                groups.remove(g.name);
                continue;
            }
            synchronized (g) {
                List<Member> dead = new ArrayList<>();
                for (Member m : g.members.values()) {
                    boolean waitingInJoin = m.join != null && !m.join.isDone();
                    if (!waitingInJoin && now - m.lastHeartbeat > props.sessionTimeoutMs()) dead.add(m);
                }
                for (Member m : dead) {
                    g.members.remove(m.id);
                    if (g.members.isEmpty()) {
                        g.state = State.EMPTY;
                        raft.note("👥 " + g.name + ": " + short_(m) + " 박동이 " + props.sessionTimeoutMs() / 1000 + "초 끊김 → 그룹이 비었음");
                    } else {
                        prepare(g, short_(m) + " 박동이 " + props.sessionTimeoutMs() / 1000 + "초 끊김(장애)", props.rebalanceTimeoutMs());
                    }
                }
                if (g.state == State.PREPARING_REBALANCE) maybeCompleteJoin(g);
                else if (g.state == State.AWAITING_SYNC && now > g.deadline) prepare(g, "리더 소비자가 배치 계획을 안 보냄", props.rebalanceTimeoutMs());
            }
        }
    }

    // ------------------------------------------------------------------ 오프셋 커밋 펜싱 · 대시보드

    /** 재조정 전의 낡은 멤버(세대가 다름)가 커밋하지 못하게 막는다. memberId 가 없으면(정적 배정) 검사하지 않는다. */
    public void checkGeneration(String group, String memberId, Integer generation) {
        if (memberId == null) return;
        Group g = groups.get(group);
        if (g == null) throw conflict("UNKNOWN_MEMBER");
        synchronized (g) {
            if (!g.members.containsKey(memberId)) throw conflict("UNKNOWN_MEMBER");
            if (generation == null || generation != g.generation) throw conflict("ILLEGAL_GENERATION");
        }
    }

    /** 대시보드 실험: 다음 박동 응답에 지시를 실어 보낸다. 이 브로커가 코디네이터면 true. */
    public boolean command(String group, String memberId, String cmd) {
        Group g = groups.get(group);
        if (g == null) return false;
        synchronized (g) {
            Member m = g.members.get(memberId);
            if (m == null) return false;
            m.command = cmd;
            return true;
        }
    }

    public List<Map<String, Object>> states() {
        long now = now();
        List<Map<String, Object>> out = new ArrayList<>();
        for (Group g : groups.values()) {
            synchronized (g) {
                Map<String, Object> gm = new LinkedHashMap<>();
                gm.put("group", g.name);
                gm.put("topic", g.topic);
                gm.put("state", g.state);
                gm.put("generation", g.generation);
                gm.put("leader", g.leader);
                List<Map<String, Object>> ms = new ArrayList<>();
                for (Member m : g.members.values()) {
                    Map<String, Object> mm = new LinkedHashMap<>();
                    mm.put("id", m.id);
                    mm.put("clientId", m.clientId);
                    mm.put("assigned", m.assigned);
                    mm.put("joined", m.joined);
                    mm.put("lastHeartbeatAgoMs", now - m.lastHeartbeat);
                    ms.add(mm);
                }
                gm.put("members", ms);
                out.add(gm);
            }
        }
        return out;
    }

    private Member member(Group g, String id) {
        Member m = id == null ? null : g.members.get(id);
        if (m == null) throw conflict("UNKNOWN_MEMBER");
        return m;
    }

    private static ResponseStatusException conflict(String code) {
        return new ResponseStatusException(HttpStatus.CONFLICT, code);
    }

    private static String short_(Member m) {
        return m == null ? "?" : m.id.substring(m.id.lastIndexOf('#'));
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
