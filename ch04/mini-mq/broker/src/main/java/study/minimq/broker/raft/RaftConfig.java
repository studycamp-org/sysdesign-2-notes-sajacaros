package study.minimq.broker.raft;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import study.minimq.broker.BrokerProperties;

@Configuration
public class RaftConfig {

    @Bean
    public RaftNode raftNode(BrokerProperties props) {
        return new RaftNode(
                props.brokerId(),
                props.members(),
                new HttpRaftTransport(),
                new FileRaftStorage(Path.of(props.dataDir())),
                new RaftNode.Timing(props.electionMinMs(), props.electionMaxMs(), props.heartbeatMs()),
                System::currentTimeMillis,
                new Random(),
                Executors.newVirtualThreadPerTaskExecutor(),
                Executors.newSingleThreadExecutor());
    }

    /**
     * 20ms 마다 Raft 시계를 돌린다. 전용 스레드를 쓰고, 그 실행기를 스프링 빈으로 내놓지 않는다.
     *
     * <p>예전에는 ScheduledExecutorService 를 빈으로 등록했는데, 그러면 스프링 @Scheduled 작업 전부가
     * 이 한 스레드에서 돌았다. 새 컨트롤러가 __consumer_offsets 를 만들며 커밋을 3초 기다리는 동안
     * 박동이 멈춰 팔로워가 다시 선거를 하고, 새 컨트롤러가 또 같은 일을 하는 순환이 생겼다(빈 클러스터 기동 때 term 64).
     */
    @Component
    static class RaftTicker {
        private final RaftNode raft;
        private ScheduledExecutorService ticker;

        RaftTicker(RaftNode raft) {
            this.raft = raft;
        }

        @PostConstruct
        void start() {
            ticker = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "raft-tick"));
            ticker.scheduleAtFixedRate(() -> {
                try {
                    raft.tick();
                } catch (Throwable t) {
                    // 여기서 던지면 ScheduledExecutorService 가 이후 실행을 조용히 멈춘다. 반드시 잡는다
                    LoggerFactory.getLogger(RaftConfig.class).error("raft tick 실패", t);
                }
            }, 20, 20, TimeUnit.MILLISECONDS);
        }

        @PreDestroy
        void stop() {
            ticker.shutdownNow();
        }
    }
}
