package study.minimq.broker.raft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

import study.minimq.common.ClusterMetadata;

/**
 * Raft-lite: 주키퍼 없이 브로커끼리 컨트롤러를 뽑고 메타데이터를 복제한다.
 *
 * <h3>Raft 에서 가져온 것</h3>
 * <ul>
 *   <li>term: 선거마다 1씩 오르는 번호. 더 높은 term 을 보면 누구든 즉시 팔로워로 내려간다</li>
 *   <li>선출: 무작위 선거 타임아웃 동안 리더 소식이 없으면 term 을 올리고 출마. 과반의 표를 받으면 리더</li>
 *   <li>한 term 에 한 표: votedFor 를 디스크에 남긴 뒤 응답한다</li>
 *   <li>선출 제한: 내 메타데이터가 후보 것보다 최신이면 표를 주지 않는다 → 커밋된 내용은 새 리더에게 반드시 있다</li>
 *   <li>커밋: 과반이 받은 버전만 커밋. 그리고 리더는 자기 term 에 만든 버전만 세어서 커밋한다</li>
 * </ul>
 *
 * <h3>단순화한 것</h3>
 * <ul>
 *   <li>로그 대신 메타데이터 스냅샷 전체를 보낸다 (작으니까)</li>
 *   <li>멤버십은 설정으로 고정 (브로커 추가·제거 없음)</li>
 *   <li>PreVote 없음 → 고립됐던 노드가 돌아오면 높아진 term 으로 리더를 끌어내린다 (일부러 보이게 남겨 둠)</li>
 * </ul>
 *
 * <p>락을 쥔 채로 네트워크를 부르지 않는다. 상태는 락 안에서 바꾸고, RPC 는 락 밖에서 보낸 뒤
 * 응답을 받으면 다시 락을 잡고 "그 사이 term 이 바뀌지 않았는지" 확인한다.
 */
public final class RaftNode {

    public enum Role {FOLLOWER, CANDIDATE, LEADER}

    public record Timing(long electionMinMs, long electionMaxMs, long heartbeatMs) {
    }

    public record NodeState(int id, Role role, long term, Integer votedFor, Integer leaderId,
                            long acceptedVersion, long committedVersion, boolean isolated, List<RaftEvent> events) {
    }

    private static final int MAX_EVENTS = 40;

    private final int id;
    private final Map<Integer, String> members;
    private final Map<Integer, String> peers;
    private final int clusterSize;
    private final RaftTransport transport;
    private final RaftStorage storage;
    private final Timing timing;
    private final LongSupplier clock;
    private final Random random;
    private final Executor io;
    private final Executor notifier;

    // 디스크에 남기는 상태
    private long currentTerm;
    private Integer votedFor;
    private ClusterMetadata accepted;  // 받았지만 아직 커밋 안 됐을 수 있는 최신 메타데이터
    private ClusterMetadata committed; // 과반이 받은 메타데이터. 브로커와 클라이언트는 이것만 본다

    // 메모리 상태
    private Role role = Role.FOLLOWER;
    private Integer leaderId;
    private long electionDeadline;
    private long nextHeartbeat;
    private int votes;
    private final Map<Integer, Long> matchVersion = new HashMap<>();
    private final Map<Integer, Long> lastAck = new HashMap<>();
    private final TreeMap<Long, CompletableFuture<ClusterMetadata>> pending = new TreeMap<>();
    private final List<Consumer<ClusterMetadata>> listeners = new CopyOnWriteArrayList<>();
    private final Deque<RaftEvent> events = new ArrayDeque<>();
    private long eventSeq;
    private volatile boolean isolated;

    public RaftNode(int id, Map<Integer, String> members, RaftTransport transport, RaftStorage storage,
                    Timing timing, LongSupplier clock, Random random, Executor io, Executor notifier) {
        this.id = id;
        this.members = new TreeMap<>(members);
        this.peers = new TreeMap<>(members);
        this.peers.remove(id);
        this.clusterSize = members.size();
        this.transport = transport;
        this.storage = storage;
        this.timing = timing;
        this.clock = clock;
        this.random = random;
        this.io = io;
        this.notifier = notifier;

        RaftStorage.State s = storage.load();
        if (s != null) {
            currentTerm = s.currentTerm();
            votedFor = s.votedFor();
            accepted = s.accepted();
            committed = s.committed();
        } else {
            accepted = committed = ClusterMetadata.initial(members);
        }
        resetElectionDeadline();
        event("시작 (term " + currentTerm + ", 메타데이터 v" + committed.version() + ")");
    }

    // ------------------------------------------------------------------ 시계

    /** 스케줄러가 짧은 주기로 부른다. 리더면 박동을, 아니면 선거 타임아웃을 확인한다. */
    public void tick() {
        long now = clock.getAsLong();
        long term;
        VoteRequest voteReq = null;
        AppendRequest appendReq = null;
        synchronized (this) {
            if (role == Role.LEADER) {
                if (now < nextHeartbeat) return;
                nextHeartbeat = now + timing.heartbeatMs();
                appendReq = new AppendRequest(currentTerm, id, accepted, committed.version(), committed);
            } else {
                if (now < electionDeadline) return;
                startElection();
                if (votes * 2 > clusterSize) { // 1대짜리 클러스터
                    becomeLeader();
                    return;
                }
                voteReq = new VoteRequest(currentTerm, id, accepted.term(), accepted.version());
            }
            term = currentTerm;
        }
        if (isolated) return; // 고립: 상태는 바뀌지만(term 증가 등) 아무에게도 닿지 않는다

        for (Map.Entry<Integer, String> e : peers.entrySet()) {
            int peer = e.getKey();
            String url = e.getValue();
            if (voteReq != null) {
                VoteRequest r = voteReq;
                io.execute(() -> {
                    try {
                        onVoteResponse(term, peer, transport.vote(url, r));
                    } catch (Exception ignored) {
                        // 응답 없음 = 표 없음
                    }
                });
            } else {
                AppendRequest r = appendReq;
                io.execute(() -> {
                    try {
                        onAppendResponse(term, peer, transport.append(url, r));
                    } catch (Exception ignored) {
                        // 응답 없음. lastAck 가 갱신되지 않아 컨트롤러가 죽은 브로커로 판단할 수 있다
                    }
                });
            }
        }
    }

    // ------------------------------------------------------------------ 선거

    private void startElection() {
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = id;
        votes = 1;
        leaderId = null;
        failPending();
        persist();
        resetElectionDeadline();
        event("선거 타임아웃 → term " + currentTerm + " 후보로 출마");
    }

    private synchronized void onVoteResponse(long term, int peer, VoteResponse r) {
        if (r.term() > currentTerm) {
            stepDown(r.term(), null, "broker-" + peer + " 에게서 더 높은 term " + r.term() + " 을 봄 → 팔로워로");
            return;
        }
        if (role != Role.CANDIDATE || currentTerm != term || !r.granted()) return;
        votes++;
        if (votes * 2 > clusterSize) becomeLeader();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        matchVersion.clear();
        long now = clock.getAsLong();
        peers.keySet().forEach(p -> lastAck.put(p, now));
        nextHeartbeat = 0;
        event("과반(" + votes + "/" + clusterSize + "표) 획득 → term " + currentTerm + " 리더 = 컨트롤러");
        // Raft: 새 리더는 자기 term 의 항목을 하나 커밋해야 이전 항목도 커밋할 수 있다.
        // 그 항목으로 "컨트롤러는 나, epoch 는 이번 term" 을 기록한다.
        proposeLocked(m -> m.withController(id, currentTerm));
    }

    public synchronized VoteResponse handleVote(VoteRequest r) {
        checkNotIsolated();
        if (r.term() < currentTerm) return new VoteResponse(currentTerm, false);
        if (r.term() > currentTerm) {
            stepDown(r.term(), null, "broker-" + r.candidateId() + " 의 더 높은 term " + r.term() + " 을 봄");
        }
        boolean upToDate = r.lastMetaTerm() > accepted.term()
                || (r.lastMetaTerm() == accepted.term() && r.lastMetaVersion() >= accepted.version());
        boolean free = votedFor == null || votedFor == r.candidateId();
        if (upToDate && free) {
            votedFor = r.candidateId();
            persist();
            resetElectionDeadline();
            event("broker-" + r.candidateId() + " 에게 투표 (term " + r.term() + ")");
            return new VoteResponse(currentTerm, true);
        }
        event("broker-" + r.candidateId() + " 에게 투표 거절: "
                + (!upToDate ? "후보의 메타데이터 v" + r.lastMetaVersion() + " 가 내 v" + accepted.version() + " 보다 뒤처짐"
                : "이번 term 에 이미 broker-" + votedFor + " 에게 투표함"));
        return new VoteResponse(currentTerm, false);
    }

    // ------------------------------------------------------------------ 복제

    public synchronized AppendResponse handleAppend(AppendRequest r) {
        checkNotIsolated();
        if (r.term() < currentTerm) {
            // 낡은 리더(좀비 컨트롤러)의 명령은 거부한다 = 펜싱. 응답의 term 을 보고 그쪽이 내려간다
            event("낡은 term " + r.term() + " 의 broker-" + r.leaderId() + " 박동 거부 (지금 term " + currentTerm + ")");
            return new AppendResponse(currentTerm, false, accepted.version());
        }
        if (r.term() > currentTerm || role != Role.FOLLOWER || !Objects.equals(leaderId, r.leaderId())) {
            stepDown(r.term(), r.leaderId(), "broker-" + r.leaderId() + " 를 리더로 따름 (term " + r.term() + ")");
        }
        resetElectionDeadline();
        if (r.metadata() != null && !r.metadata().equals(accepted)) {
            accepted = r.metadata(); // 리더의 스냅샷이 이번 term 의 정답이다
            persist();
        }
        if (r.commitVersion() >= accepted.version() && !committed.equals(accepted)) {
            commit(accepted);
        } else if (r.committed() != null && r.committed().version() > committed.version()) {
            commit(r.committed());
        }
        return new AppendResponse(currentTerm, true, accepted.version());
    }

    private synchronized void onAppendResponse(long term, int peer, AppendResponse r) {
        if (r.term() > currentTerm) {
            stepDown(r.term(), null, "broker-" + peer + " 가 더 높은 term " + r.term() + " 을 알려 줌 → 컨트롤러에서 내려옴");
            return;
        }
        if (role != Role.LEADER || currentTerm != term) return;
        lastAck.put(peer, clock.getAsLong());
        if (r.success()) {
            matchVersion.put(peer, r.acceptedVersion());
            maybeCommit();
        }
    }

    /** 메타데이터를 바꾼다. 과반이 받으면 future 가 커밋된 메타데이터로 완료된다. 컨트롤러만 부를 수 있다. */
    public synchronized CompletableFuture<ClusterMetadata> propose(UnaryOperator<ClusterMetadata> change) {
        if (role != Role.LEADER) return CompletableFuture.failedFuture(new NotControllerException(leaderId));
        return proposeLocked(change);
    }

    private CompletableFuture<ClusterMetadata> proposeLocked(UnaryOperator<ClusterMetadata> change) {
        accepted = change.apply(accepted).withVersion(accepted.version() + 1, currentTerm);
        persist();
        CompletableFuture<ClusterMetadata> f = new CompletableFuture<>();
        pending.put(accepted.version(), f);
        nextHeartbeat = 0; // 다음 tick 에 바로 복제
        maybeCommit();
        return f;
    }

    private void maybeCommit() {
        if (role != Role.LEADER || committed.version() >= accepted.version()) return;
        if (accepted.term() != currentTerm) return; // 이전 term 의 것은 직접 세지 않는다 (Raft 그림 8 문제)
        long v = accepted.version();
        int count = 1;
        for (int p : peers.keySet()) if (matchVersion.getOrDefault(p, -1L) >= v) count++;
        if (count * 2 <= clusterSize) return;
        commit(accepted);
        var done = pending.headMap(v, true);
        done.values().forEach(f -> f.complete(committed));
        done.clear();
    }

    private void commit(ClusterMetadata m) {
        committed = m;
        persist();
        event("메타데이터 v" + m.version() + " 커밋");
        ClusterMetadata snapshot = m;
        notifier.execute(() -> listeners.forEach(l -> l.accept(snapshot)));
    }

    // ------------------------------------------------------------------ 도우미

    private void stepDown(long term, Integer newLeader, String reason) {
        boolean changed = term > currentTerm || role != Role.FOLLOWER || !Objects.equals(leaderId, newLeader);
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
        }
        role = Role.FOLLOWER;
        leaderId = newLeader;
        failPending();
        persist();
        resetElectionDeadline();
        if (changed) event(reason);
    }

    private void failPending() {
        // 리더 자리를 잃으면 커밋 여부를 알 수 없다. 나중에 새 리더가 커밋할 수도 있다(결과 미상)
        pending.values().forEach(f -> f.completeExceptionally(new NotControllerException(leaderId)));
        pending.clear();
    }

    private void resetElectionDeadline() {
        long span = timing.electionMaxMs() - timing.electionMinMs();
        electionDeadline = clock.getAsLong() + timing.electionMinMs() + (span > 0 ? (long) (random.nextDouble() * span) : 0);
    }

    private void persist() {
        storage.save(new RaftStorage.State(currentTerm, votedFor, accepted, committed));
    }

    private void checkNotIsolated() {
        if (isolated) throw new IsolatedException();
    }

    private void event(String text) {
        events.addFirst(new RaftEvent(clock.getAsLong(), ++eventSeq, id, currentTerm, text));
        while (events.size() > MAX_EVENTS) events.removeLast();
    }

    // ------------------------------------------------------------------ 바깥에서 보는 것

    /** 커밋된 메타데이터가 바뀔 때마다 불린다. 등록하자마자 지금 값으로 한 번 불린다. */
    public void addListener(Consumer<ClusterMetadata> listener) {
        listeners.add(listener);
        ClusterMetadata now = committed();
        notifier.execute(() -> listener.accept(now));
    }

    public synchronized void note(String text) {
        event(text);
    }

    public synchronized boolean isLeader() {
        return role == Role.LEADER;
    }

    public synchronized ClusterMetadata committed() {
        return committed;
    }

    public synchronized ClusterMetadata accepted() {
        return accepted;
    }

    /** 리더일 때, 각 브로커에서 마지막으로 응답을 받은 뒤 지난 시간(ms). 리더가 아니면 빈 맵. */
    public synchronized Map<Integer, Long> peerAckAges() {
        if (role != Role.LEADER) return Map.of();
        long now = clock.getAsLong();
        Map<Integer, Long> out = new TreeMap<>();
        for (int p : peers.keySet()) out.put(p, now - lastAck.getOrDefault(p, 0L));
        return out;
    }

    public synchronized String leaderUrl() {
        return leaderId == null ? null : members.get(leaderId);
    }

    public synchronized Integer leaderId() {
        return leaderId;
    }

    public int id() {
        return id;
    }

    public Map<Integer, String> members() {
        return members;
    }

    public void setIsolated(boolean on) {
        isolated = on;
        note(on ? "🔌 고립 시작: 아무와도 이야기할 수 없음" : "🔗 고립 해제: 다시 연결됨");
    }

    public boolean isolated() {
        return isolated;
    }

    public synchronized NodeState state() {
        return new NodeState(id, role, currentTerm, votedFor, leaderId, accepted.version(), committed.version(),
                isolated, new ArrayList<>(events));
    }
}
