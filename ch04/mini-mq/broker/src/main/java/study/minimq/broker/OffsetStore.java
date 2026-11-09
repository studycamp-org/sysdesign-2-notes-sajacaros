package study.minimq.broker;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import study.minimq.broker.zk.Coordination;
import study.minimq.common.OffsetCommit;

/**
 * 상태 저장소: 소비자 그룹별·파티션별 마지막으로 소비한 오프셋 (그림 4.21, 4.22).
 *
 * <p>교재처럼 조정 서비스(ZooKeeper-lite)에 둔다: /offsets/{그룹}/{토픽}/{파티션} = 오프셋.
 * 소비자는 그룹 코디네이터 브로커에 커밋하고, 코디네이터가 세대를 확인한 뒤 조정 서비스에 쓴다.
 * 브로커가 죽어도 오프셋은 조정 서비스에 남는다.
 */
@Component
public class OffsetStore {
    private final Coordination coord;
    private final Traces traces;
    private final BrokerProperties props;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public OffsetStore(Coordination coord, Traces traces, BrokerProperties props) {
        this.coord = coord;
        this.traces = traces;
        this.props = props;
    }

    /** 한 그룹의 여러 파티션 오프셋을 조정 서비스에 쓴다. */
    public CompletableFuture<Void> commit(List<OffsetCommit> commits) {
        if (commits.isEmpty()) return CompletableFuture.completedFuture(null);
        String group = commits.get(0).group();
        if (commits.stream().anyMatch(c -> !c.group().equals(group))) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "한 번에 한 그룹만");
        }
        return CompletableFuture.runAsync(() -> {
            try {
                for (OffsetCommit c : commits) {
                    coord.putOffset(c.group(), c.topic(), c.partition(), c.offset());
                    traces.hit(c.topic(), c.partition(), 0, c.offset() + 1, "committed", "group " + c.group(),
                            "그룹 " + c.group() + " 가 offset " + c.offset() + " 까지 처리했다고 커밋 → 코디네이터 broker-" + props.brokerId()
                                    + " 가 /offsets/" + c.group() + "/" + c.topic() + "/" + c.partition() + " = " + c.offset()
                                    + " 로 기록 (그룹이 다시 시작하면 " + (c.offset() + 1) + " 부터 읽는다)");
                }
            } catch (IOException e) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "조정 서비스에 오프셋을 못 씀: " + e.getMessage());
            }
        }, pool);
    }

    /** 그룹이 이 토픽에서 파티션별로 마지막으로 소비한 오프셋. 없는 파티션은 빠진다. */
    public Map<Integer, Long> committed(String group, String topic) {
        try {
            return coord.offsets(group, topic);
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "조정 서비스에서 오프셋을 못 읽음: " + e.getMessage());
        }
    }
}
