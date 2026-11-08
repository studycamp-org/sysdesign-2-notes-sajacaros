package study.minimq.broker;

import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class BrokerApplication {
    static final String DOWN_UNTIL = "down-until";

    public static void main(String[] args) throws Exception {
        waitIfCrashed();
        SpringApplication.run(BrokerApplication.class, args);
    }

    /** 대시보드의 "강제 종료"는 꺼져 있을 시각을 남기고 죽는다. 컨테이너가 바로 다시 띄워도 그때까지 기다린다. */
    private static void waitIfCrashed() throws Exception {
        String dir = System.getenv().getOrDefault("MINIMQ_DATA_DIR", "./data/broker-1");
        Path marker = Path.of(dir).resolve(DOWN_UNTIL);
        if (!Files.exists(marker)) return;
        long until = Long.parseLong(Files.readString(marker).trim());
        Files.delete(marker);
        long wait = until - System.currentTimeMillis();
        if (wait > 0) {
            System.out.println("강제 종료 실험: " + wait / 1000 + "초 더 꺼져 있는다");
            Thread.sleep(wait);
        }
    }
}
