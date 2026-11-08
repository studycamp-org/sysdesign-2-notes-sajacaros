package study.minimq.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** 리더 소비자가 만드는 파티션 배치 계획 (그림 4.18 의 "A 에 1·3, B 에 2·4" 같은 것). */
class AssignorTest {

    @Test
    void range_는_연속_구간을_나눠_준다() {
        Map<String, List<Integer>> plan = Consumer.assign(List.of("b", "a", "c"), 7, "range");
        assertThat(plan).containsEntry("a", List.of(0, 1, 2)).containsEntry("b", List.of(3, 4)).containsEntry("c", List.of(5, 6));
    }

    @Test
    void roundrobin_은_번갈아_나눠_준다() {
        Map<String, List<Integer>> plan = Consumer.assign(List.of("a", "b"), 4, "roundrobin");
        assertThat(plan).containsEntry("a", List.of(0, 2)).containsEntry("b", List.of(1, 3)); // 교재 그림 4.18 과 같은 모양
    }

    @Test
    void 파티션보다_소비자가_많으면_남는_소비자는_논다() {
        Map<String, List<Integer>> plan = Consumer.assign(List.of("a", "b", "c", "d"), 3, "range");
        assertThat(plan.values().stream().filter(List::isEmpty).count()).isEqualTo(1);
        assertThat(plan.values().stream().mapToInt(List::size).sum()).isEqualTo(3);
    }
}
