package study.minimq.demo;

import java.io.IOException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import study.minimq.client.ClusterView;
import study.minimq.common.ClusterMetadata;
import study.minimq.common.TopicInfo;

/** 컨테이너가 브로커보다 먼저 뜨거나 컨트롤러 선출 중일 수 있으니 기다린다. */
final class Startup {
    private static final Logger log = LoggerFactory.getLogger(Startup.class);

    private Startup() {
    }

    /** 컨트롤러가 뽑히고 토픽이 만들어질 때까지 (아무 브로커에나 요청해도 컨트롤러에게 전달된다). */
    static void createTopic(String bootstrap, String topic, int partitions) throws InterruptedException {
        ClusterView view = new ClusterView(bootstrap);
        while (true) {
            try {
                ClusterMetadata m = view.refresh();
                if (m.controllerId() < 0) {
                    log.info("컨트롤러 선출을 기다리는 중...");
                } else {
                    view.any().post("/topics", new TopicInfo(topic, partitions), TopicInfo.class, Duration.ofSeconds(5));
                    log.info("토픽 {} 준비됨 (컨트롤러 broker-{})", topic, m.controllerId());
                    return;
                }
            } catch (IOException e) {
                log.info("클러스터를 기다리는 중... ({})", e.getMessage());
            }
            Thread.sleep(1_000);
        }
    }

    static void waitForTopic(String bootstrap, String topic) throws InterruptedException {
        ClusterView view = new ClusterView(bootstrap);
        while (true) {
            try {
                if (view.refresh().topics().containsKey(topic)) return;
                log.info("토픽 {} 을 기다리는 중...", topic);
            } catch (IOException e) {
                log.info("클러스터를 기다리는 중... ({})", e.getMessage());
            }
            Thread.sleep(1_000);
        }
    }
}
