package study.minimq.broker.raft;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

    /** 20ms 마다 시계를 돌린다. 스프링 스케줄러와 따로 둬서 다른 작업에 밀리지 않게 한다. */
    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService raftTicker(RaftNode raft) {
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "raft-tick"));
        ticker.scheduleAtFixedRate(() -> {
            try {
                raft.tick();
            } catch (Exception ignored) {
                // tick 이 죽으면 선거도 박동도 멈추므로 삼킨다
            }
        }, 20, 20, TimeUnit.MILLISECONDS);
        return ticker;
    }
}
