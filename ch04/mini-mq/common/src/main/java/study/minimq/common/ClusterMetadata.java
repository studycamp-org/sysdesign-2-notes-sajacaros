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
 * @param settings        클러스터 설정 (예: 데모 자동 생산 켜고 끄기). 메타데이터와 함께 과반에 복제된다
 */
public record ClusterMetadata(
        long version,
        long term,
        int controllerId,
        long controllerEpoch,
        Map<Integer, BrokerInfo> brokers,
        Map<String, TopicMetadata> topics,
        Map<String, String> settings) {

    public ClusterMetadata {
        if (settings == null) settings = Map.of(); // 예전 raft.json 에는 없다
    }

    public static ClusterMetadata initial(Map<Integer, String> members) {
        Map<Integer, BrokerInfo> brokers = new TreeMap<>();
        members.forEach((id, url) -> brokers.put(id, new BrokerInfo(id, url, true)));
        return new ClusterMetadata(0, 0, -1, 0, brokers, new TreeMap<>(), Map.of());
    }

    public ClusterMetadata withVersion(long newVersion, long newTerm) {
        return new ClusterMetadata(newVersion, newTerm, controllerId, controllerEpoch, brokers, topics, settings);
    }

    public ClusterMetadata withController(int id, long epoch) {
        return new ClusterMetadata(version, term, id, epoch, brokers, topics, settings);
    }

    /** 브로커를 지도에 넣거나 주소를 고친다 (조정 서비스 /brokers/{id} 에 등록된 주소). */
    public ClusterMetadata withBroker(int id, String url, boolean alive) {
        Map<Integer, BrokerInfo> b = new TreeMap<>(brokers);
        b.put(id, new BrokerInfo(id, url, alive));
        return new ClusterMetadata(version, term, controllerId, controllerEpoch, b, topics, settings);
    }

    public ClusterMetadata withBrokerAlive(int id, boolean alive) {
        Map<Integer, BrokerInfo> b = new TreeMap<>(brokers);
        BrokerInfo old = b.get(id);
        if (old != null) b.put(id, new BrokerInfo(id, old.url(), alive));
        return new ClusterMetadata(version, term, controllerId, controllerEpoch, b, topics, settings);
    }

    public ClusterMetadata withTopic(TopicMetadata t) {
        Map<String, TopicMetadata> m = new TreeMap<>(topics);
        m.put(t.name(), t);
        return new ClusterMetadata(version, term, controllerId, controllerEpoch, brokers, m, settings);
    }

    public ClusterMetadata withPartition(String topic, PartitionMetadata p) {
        return withTopic(topics.get(topic).withPartition(p));
    }

    public ClusterMetadata withSetting(String key, String value) {
        Map<String, String> m = new TreeMap<>(settings);
        m.put(key, value);
        return new ClusterMetadata(version, term, controllerId, controllerEpoch, brokers, topics, m);
    }

    public String setting(String key, String fallback) {
        return settings.getOrDefault(key, fallback);
    }

    public Optional<PartitionMetadata> partition(String topic, int partition) {
        TopicMetadata t = topics.get(topic);
        if (t == null || partition < 0 || partition >= t.partitions().size()) return Optional.empty();
        return Optional.of(t.partitions().get(partition));
    }

    /** 파티션 리더 브로커의 주소. 없으면 null. */
    public String leaderUrl(String topic, int partition) {
        return partition(topic, partition).filter(p -> p.leader() >= 0)
                .map(p -> brokers.get(p.leader())).map(BrokerInfo::url).orElse(null);
    }
}
