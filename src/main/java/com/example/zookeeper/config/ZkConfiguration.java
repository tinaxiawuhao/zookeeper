package com.example.zookeeper.config;

import com.example.zookeeper.client.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZkConfiguration {

    @Bean(initMethod = "init", destroyMethod = "stop")
    public ZkClient zkClient(ZookeeperProperties zookeeperProperties) {
        return new ZkClient(zookeeperProperties);
    }
}