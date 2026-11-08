package study.minimq.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import study.minimq.common.ClusterMetadata;
import study.minimq.common.PartitionMetadata;
import study.minimq.common.TopicMetadata;

/** 그림 4.28: 브로커가 죽으면 ISR 에서 새 리더를 뽑는다. */
class ControllerFailoverTest {

    private static ClusterMetadata cluster(PartitionMetadata... parts) {
        return ClusterMetadata.initial(Map.of(1, "a", 2, "b", 3, "c"))
                .withTopic(new TopicMetadata("t", List.of(parts), 3, 2));
    }

    @Test
    void 리더가_죽으면_ISR_안의_살아_있는_사본이_리더가_되고_epoch_가_오른다() {
        ClusterMetadata m = cluster(new PartitionMetadata(0, 1, 4, List.of(1, 2, 3), List.of(1, 2, 3)));
        PartitionMetadata p = Controller.onBrokerDead(m.withBrokerAlive(1, false), 1).partition("t", 0).orElseThrow();
        assertThat(p.leader()).isEqualTo(2);
        assertThat(p.leaderEpoch()).isEqualTo(5);
        assertThat(p.isr()).containsExactly(2, 3);
    }

    @Test
    void ISR_밖의_사본은_리더가_될_수_없다() {
        // broker-2 는 뒤처져 ISR 에서 빠져 있었다
        ClusterMetadata m = cluster(new PartitionMetadata(0, 1, 0, List.of(1, 2, 3), List.of(1, 3)));
        PartitionMetadata p = Controller.onBrokerDead(m.withBrokerAlive(1, false), 1).partition("t", 0).orElseThrow();
        assertThat(p.leader()).isEqualTo(3);
    }

    @Test
    void ISR_이_모두_죽으면_리더_없음으로_두고_마지막_리더가_돌아오면_맡긴다() {
        ClusterMetadata m = cluster(new PartitionMetadata(0, 1, 0, List.of(1, 2, 3), List.of(1)));
        ClusterMetadata dead = Controller.onBrokerDead(m.withBrokerAlive(1, false), 1);
        assertThat(dead.partition("t", 0).orElseThrow().leader()).isEqualTo(-1);
        assertThat(dead.leaderUrl("t", 0)).isNull();

        PartitionMetadata back = Controller.onBrokerAlive(dead.withBrokerAlive(1, true), 1).partition("t", 0).orElseThrow();
        assertThat(back.leader()).isEqualTo(1);
        assertThat(back.leaderEpoch()).isEqualTo(2);
    }

    @Test
    void 팔로워가_죽으면_ISR_에서만_빠지고_리더는_그대로() {
        ClusterMetadata m = cluster(new PartitionMetadata(0, 1, 0, List.of(1, 2, 3), List.of(1, 2, 3)));
        PartitionMetadata p = Controller.onBrokerDead(m.withBrokerAlive(3, false), 3).partition("t", 0).orElseThrow();
        assertThat(p.leader()).isEqualTo(1);
        assertThat(p.leaderEpoch()).isEqualTo(0);
        assertThat(p.isr()).containsExactly(1, 2);
    }
}
