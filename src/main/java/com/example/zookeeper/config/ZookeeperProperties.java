package com.example.zookeeper.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Configuration
@ConfigurationProperties(prefix = "zookeeper", ignoreUnknownFields = true)
@PropertySource("classpath:application.properties")
@Data
@Component
@NoArgsConstructor
@AllArgsConstructor
public class ZookeeperProperties {
    private Boolean enabled;
    private String server;
    private String namespace;
    //zkCli.sh acl 命令 addauth digest mpush
    private String digest;
    //会话超时时间，单位为毫秒，默认60000ms,连接断开后，其它客户端还能请到临时节点的时间
    private Integer sessionTimeoutMs;
    //连接创建超时时间，单位为毫秒
    private Integer connectionTimeoutMs;
    //最大重试次数
    private Integer maxRetries;
    //初始sleep时间 ,毫秒
    private Integer baseSleepTimeMs;
}
