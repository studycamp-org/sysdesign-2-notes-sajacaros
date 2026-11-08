package study.minimq.common;

import java.util.List;

/** 토픽 이름 규칙과 그룹 코디네이터 정하기. */
public final class Topics {
    private Topics() {
    }

    /** __ 로 시작하면 내부용 (보관 기간으로 지우지 않는다). */
    public static boolean internal(String topic) {
        return topic.startsWith("__");
    }

    /**
     * 그룹 코디네이터 (그림 4.15: 그룹 이름을 해싱해 전담 브로커를 찾는다).
     * 살아 있는 브로커를 번호순으로 늘어놓고 hash(그룹) % 개수 번째. 지도만 있으면 누구나 같은 답을 낸다.
     * 브로커가 죽거나 돌아오면 바뀔 수 있다 → 그 그룹은 새 코디네이터에 다시 합류한다.
     */
    public static int coordinatorFor(String group, ClusterMetadata m) {
        List<Integer> alive = m.brokers().values().stream().filter(BrokerInfo::alive).map(BrokerInfo::id).sorted().toList();
        return alive.isEmpty() ? -1 : alive.get(Partitioner.forKey(group, alive.size()));
    }

    /** ZooKeeper-lite 의 오프셋 경로 (상태 저장소, 그림 4.22). */
    public static String offsetPath(String group, String topic) {
        return "/offsets/" + group + "/" + topic;
    }
}
