package study.minimq.demo;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.client.MiniMqHttp;
import study.minimq.common.TopicInfo;

/** 컨테이너가 브로커보다 먼저 뜰 수 있으니 기다린다. */
final class Startup {
    private static final Logger log = LoggerFactory.getLogger(Startup.class);

    private Startup() {
    }

    static void waitForBroker(String bootstrap) throws InterruptedException {
        waitUntil(bootstrap, null);
    }

    static void waitForTopic(String bootstrap, String topic) throws InterruptedException {
        waitUntil(bootstrap, topic);
    }

    private static void waitUntil(String bootstrap, String topic) throws InterruptedException {
        MiniMqHttp http = new MiniMqHttp(bootstrap);
        while (true) {
            try {
                List<TopicInfo> topics = http.get("/topics", new TypeReference<List<TopicInfo>>() {}, Duration.ofSeconds(2));
                if (topic == null || topics.stream().anyMatch(t -> t.name().equals(topic))) return;
                log.info("토픽 {} 을 기다리는 중...", topic);
            } catch (IOException e) {
                log.info("브로커 {} 를 기다리는 중... ({})", bootstrap, e.getMessage());
            }
            Thread.sleep(1_000);
        }
    }
}
