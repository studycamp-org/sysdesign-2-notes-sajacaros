package study.minimq.broker.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import study.minimq.common.ClusterMetadata;
import study.minimq.common.PartitionMetadata;
import study.minimq.common.TopicMetadata;

/** 가짜 시계 + 메모리 네트워크로 3노드 클러스터를 결정론적으로 돌린다. */
class RaftNodeTest {

    private final AtomicLong now = new AtomicLong(1_000);
    private final Map<String, RaftNode> byUrl = new TreeMap<>();
    private final Map<Integer, RaftNode> nodes = new TreeMap<>();
    private final Set<Integer> down = new HashSet<>(); // 네트워크에서 끊긴 노드

    @BeforeEach
    void setUp() {
        Map<Integer, String> members = Map.of(1, "n1", 2, "n2", 3, "n3");
        RaftTransport net = new RaftTransport() {
            @Override
            public VoteResponse vote(String url, VoteRequest req) throws IOException {
                return target(url, req.candidateId()).handleVote(req);
            }

            @Override
            public AppendResponse append(String url, AppendRequest req) throws IOException {
                return target(url, req.leaderId()).handleAppend(req);
            }
        };
        for (int id : members.keySet()) {
            RaftNode n = new RaftNode(id, members, net, new MemoryStorage(),
                    new RaftNode.Timing(150, 300, 50), now::get, new Random(id), Runnable::run, Runnable::run);
            nodes.put(id, n);
            byUrl.put("n" + id, n);
        }
    }

    private RaftNode target(String url, int from) throws IOException {
        RaftNode t = byUrl.get(url);
        if (down.contains(from) || down.contains(t.id())) throw new IOException("끊김");
        return t;
    }

    private void runUntil(BooleanSupplier done) {
        for (int i = 0; i < 2_000 && !done.getAsBoolean(); i++) {
            now.addAndGet(10);
            nodes.forEach((id, n) -> {
                if (!down.contains(id)) n.tick();
            });
        }
        assertThat(done.getAsBoolean()).as("조건이 시간 안에 만족되지 않음").isTrue();
    }

    private void runFor(long ms) {
        for (long t = 0; t < ms; t += 10) {
            now.addAndGet(10);
            nodes.forEach((id, n) -> {
                if (!down.contains(id)) n.tick();
            });
        }
    }

    private List<RaftNode> leaders() {
        return nodes.values().stream().filter(n -> !down.contains(n.id()) && n.isLeader()).toList();
    }

    private static ClusterMetadata withTopic(ClusterMetadata m, String name) {
        return m.withTopic(new TopicMetadata(name, List.of(new PartitionMetadata(0, 1, 0, List.of(1), List.of(1))), 1, 1));
    }

    @Test
    void 리더는_하나만_뽑히고_모두가_컨트롤러를_안다() {
        runUntil(() -> leaders().size() == 1);
        RaftNode leader = leaders().get(0);
        runUntil(() -> nodes.values().stream().allMatch(n -> n.committed().controllerId() == leader.id()));

        assertThat(nodes.values()).allMatch(n -> n.committed().controllerEpoch() == leader.state().term());
        runFor(2_000); // 박동이 오는 동안은 다시 선거하지 않는다
        assertThat(leaders()).containsExactly(leader);
    }

    @Test
    void 과반이_받아야_커밋된다() {
        runUntil(() -> leaders().size() == 1);
        RaftNode leader = leaders().get(0);
        runUntil(() -> leader.committed().controllerId() == leader.id());

        // 팔로워 둘을 끊으면 1/3 이라 커밋이 안 된다
        nodes.keySet().stream().filter(id -> id != leader.id()).forEach(down::add);
        CompletableFuture<ClusterMetadata> f = leader.propose(m -> withTopic(m, "orders"));
        runFor(1_000);
        assertThat(f).isNotDone();
        assertThat(leader.committed().topics()).doesNotContainKey("orders");
        assertThat(leader.accepted().topics()).containsKey("orders");
    }

    @Test
    void 리더가_죽으면_더_높은_term_의_새_리더가_뽑히고_커밋된_내용은_남는다() {
        runUntil(() -> leaders().size() == 1);
        RaftNode old = leaders().get(0);
        CompletableFuture<ClusterMetadata> f = old.propose(m -> withTopic(m, "orders"));
        runUntil(f::isDone);
        long oldTerm = old.state().term();

        down.add(old.id());
        runUntil(() -> leaders().size() == 1);
        RaftNode neo = leaders().get(0);
        assertThat(neo.id()).isNotEqualTo(old.id());
        assertThat(neo.state().term()).isGreaterThan(oldTerm);
        // 옛 리더가 커밋을 알리기 전에 죽었어도, 과반이 받은 것은 새 리더에게 있다(선출 제한).
        // 새 리더가 자기 term 의 항목을 커밋하는 순간 함께 커밋된다.
        assertThat(neo.accepted().topics()).containsKey("orders");
        runUntil(() -> neo.committed().controllerId() == neo.id());
        assertThat(neo.committed().topics()).containsKey("orders");

        // 옛 리더가 돌아오면 높은 term 을 보고 내려오고 새 메타데이터를 따른다
        down.remove(old.id());
        runUntil(() -> !old.isLeader() && old.committed().controllerId() == neo.id());
        assertThat(old.committed().topics()).containsKey("orders");
    }

    @Test
    void 고립된_옛_리더의_커밋_안_된_변경은_버려진다() {
        runUntil(() -> leaders().size() == 1);
        RaftNode zombie = leaders().get(0);
        runUntil(() -> zombie.committed().controllerId() == zombie.id());

        down.add(zombie.id());
        CompletableFuture<ClusterMetadata> lost = zombie.propose(m -> withTopic(m, "zombie-topic"));
        // 끊긴 쪽만 시간이 흐르게 하면 안 되므로, 끊긴 노드도 tick 은 하되 아무에게도 닿지 않게 한다
        for (int i = 0; i < 100; i++) {
            now.addAndGet(10);
            nodes.values().forEach(RaftNode::tick);
        }
        assertThat(lost).isNotDone();
        runUntil(() -> nodes.values().stream().anyMatch(n -> n != zombie && n.isLeader()));

        down.remove(zombie.id());
        runUntil(() -> !zombie.isLeader() && zombie.committed().controllerId() != zombie.id());
        assertThat(lost).isCompletedExceptionally();
        assertThat(zombie.accepted().topics()).doesNotContainKey("zombie-topic");
    }

    @Test
    void 메타데이터가_뒤처진_후보는_표를_못_받는다() {
        RaftNode n1 = nodes.get(1);
        // n1 은 term 5 에 v3 을 가졌다고 하자. 후보는 term 6 이지만 v1(term 2) 만 가졌다
        n1.handleAppend(new AppendRequest(5, 2, ClusterMetadata.initial(Map.of(1, "n1", 2, "n2", 3, "n3")).withVersion(3, 5), 0, null));
        VoteResponse r = n1.handleVote(new VoteRequest(6, 3, 2, 1));
        assertThat(r.granted()).isFalse();
        assertThat(r.term()).isEqualTo(6);

        // 같은 term 에 더 최신 후보에게는 준다
        assertThat(n1.handleVote(new VoteRequest(6, 2, 5, 3)).granted()).isTrue();
        // 같은 term 에 두 번은 안 준다
        assertThat(n1.handleVote(new VoteRequest(6, 3, 5, 3)).granted()).isFalse();
    }

    @Test
    void 변경이_쉬지_않고_들어와도_팔로워가_커밋을_따라온다() {
        runUntil(() -> leaders().size() == 1);
        RaftNode leader = leaders().get(0);
        runUntil(() -> leader.committed().controllerId() == leader.id());
        // 박동마다 새 변경을 넣어 팔로워가 받는 스냅샷이 항상 커밋보다 앞서게 만든다
        for (int i = 0; i < 50; i++) {
            int n = i;
            leader.propose(m -> withTopic(m, "t" + n));
            now.addAndGet(60);
            nodes.values().forEach(RaftNode::tick);
        }
        assertThat(nodes.values()).allMatch(n -> n.committed().version() >= leader.committed().version() - 2);
    }

    static final class MemoryStorage implements RaftStorage {
        private State state;

        @Override
        public State load() {
            return state;
        }

        @Override
        public void save(State s) {
            state = s;
        }
    }
}
