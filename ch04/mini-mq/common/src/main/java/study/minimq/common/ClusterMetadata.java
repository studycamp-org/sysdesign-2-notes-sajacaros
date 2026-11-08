package study.minimq.common;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 메타데이터 저장소의 내용 (교재 그림 4.8). 컨트롤러가 그리는 "지도"이고, 생산자·소비자는 이걸 보고 리더 브로커를 찾아간다.
 *
 * <p>Raft-lite 는 로그 대신 이 스냅샷 전체를 복제한다.
 * {@code version} 은 바뀔 때마다 1씩 늘고, {@code term} 은 그 버전을 만든 컨트롤러의 term 이다.
 * 투표할 때 (term, version) 을 비교해 더 뒤처진 후보에게는 표를 주지 않는다.
 *
 * @param controllerEpoch 지금 컨트롤러가 당선된 term. 브로커는 이보다 낮은 epoch 의 명령을 거부한다(펜싱)
 */
public record ClusterMetadata(
        long version,
        long term,
        int controllerId,
        long controllerEpoch,
        Map<Integer, BrokerInfo> brokers,
        Map<String, TopicMetadata> topics) {

    public static ClusterMetadata initial(Map<Integer, String> members) {
        Map<Integer, BrokerInfo> brokers = new TreeMap<>();
        members.forEach((id, url) -> brokers.put(id, new BrokerInfo(id, url, true)));
        return new ClusterMetadata(0, 0, -1, 0, brokers, new TreeMap<>());
    }

    public ClusterMetadata withVersion(long newVersion, long newTerm) {
        return new ClusterMetadata(newVersion, newTerm, controllerId, controllerEpoch, brokers, topics);
    }

    public ClusterMetadata withController(int id, long epoch) {
        return new ClusterMetadata(version, term, id, epoch, brokers, topics);
    }

    public ClusterMetadata withBrokerAlive(int id, boolean alive) {
        Map<Integer, BrokerInfo> b = new TreeMap<>(brokers);
        BrokerInfo old = b.get(id);
        if (old != null) b.put(id, new BrokerInfo(id, old.url(), alive));
        return new ClusterMetadata(version, term, controllerId, controllerEpoch, b, topics);
    }

    public ClusterMetadata withTopic(TopicMetadata t) {
        Map<String, TopicMetadata> m = new TreeMap<>(topics);
        m.put(t.name(), t);
        return new ClusterMetadata(version, term, controllerId, controllerEpoch, brokers, m);
    }

    public Optional<PartitionMetadata> partition(String topic, int partition) {
        TopicMetadata t = topics.get(topic);
        if (t == null || partition < 0 || partition >= t.partitions().size()) return Optional.empty();
        return Optional.of(t.partitions().get(partition));
    }

    /** 파티션 리더 브로커의 주소. 없으면 null. */
    public String leaderUrl(String topic, int partition) {
        return partition(topic, partition).map(p -> brokers.get(p.leader())).map(BrokerInfo::url).orElse(null);
    }
}
