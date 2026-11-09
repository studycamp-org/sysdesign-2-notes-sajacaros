package study.minimq.broker.zk;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import study.minimq.broker.BrokerProperties;

@Configuration
public class CoordinationConfig {
    @Bean
    public Coordination coordination(BrokerProperties props) {
        Coordination c = new Coordination(props.brokerId(), props.advertisedUrl(), props.zkUrl());
        c.start();
        return c;
    }
}
