package study.minimq.zk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class ZkStoreTest {
    private final AtomicLong now = new AtomicLong(1_000);
    private final ZkStore zk = new ZkStore(null, 4_000, now::get);

    @Test
    void 임시_노드를_먼저_만든_쪽이_컨트롤러가_되고_세션이_끝나면_자리가_빈다() {
        var s1 = zk.openSession("broker-1");
        var s2 = zk.openSession("broker-2");
        var c = zk.create("/controller", "{\"id\":1}", s1.id(), "broker-1");
        assertThatThrownBy(() -> zk.create("/controller", "{\"id\":2}", s2.id(), "broker-2"))
                .hasMessageContaining("NODE_EXISTS");

        // broker-1 의 박동이 끊김
        now.addAndGet(5_000);
        zk.ping(s2.id());
        zk.expireSessions();
        assertThatThrownBy(() -> zk.get("/controller")).hasMessageContaining("NO_NODE");

        var c2 = zk.create("/controller", "{\"id\":2}", s2.id(), "broker-2");
        assertThat(c2.czxid()).isGreaterThan(c.czxid()); // 새 컨트롤러 epoch 가 더 크다
    }

    @Test
    void 버전이_다르면_고칠_수_없다() {
        zk.create("/metadata", "v0", null, "x");
        zk.set("/metadata", "v1", 0, null, null, "x");
        assertThatThrownBy(() -> zk.set("/metadata", "v1'", 0, null, null, "y")).hasMessageContaining("BAD_VERSION");
        assertThat(zk.get("/metadata").data()).isEqualTo("v1");
    }

    @Test
    void 낡은_컨트롤러는_메타데이터를_고칠_수_없다() {
        var s1 = zk.openSession("broker-1");
        var s2 = zk.openSession("broker-2");
        zk.create("/metadata", "m", null, "x");
        long epoch1 = zk.create("/controller", "1", s1.id(), "broker-1").czxid();
        zk.closeSession(s1.id());
        zk.create("/controller", "2", s2.id(), "broker-2");
        assertThatThrownBy(() -> zk.set("/metadata", "zombie", -1, "/controller", epoch1, "broker-1"))
                .hasMessageContaining("CHECK_FAILED");
    }

    @Test
    void 감시는_바뀌면_깨어난다() {
        long z = zk.zxid();
        CompletableFuture<Long> w = zk.waitForChange(z);
        assertThat(w).isNotDone();
        zk.create("/brokers", null, null, "x");
        assertThat(w).isDone();
        assertThat(zk.children("/")).contains("brokers");
    }
}
