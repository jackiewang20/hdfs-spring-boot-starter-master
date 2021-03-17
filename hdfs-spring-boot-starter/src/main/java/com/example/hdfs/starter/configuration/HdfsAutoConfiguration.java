package com.example.hdfs.starter.configuration;

import com.example.hdfs.starter.component.HdfsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author jackie wang
 * @since 2021/3/16 20:11
 */
@Configuration
@EnableConfigurationProperties(HdfsProperties.class)
public class HdfsAutoConfiguration {

    @Autowired
    private HdfsProperties properties;

    @Bean
    @ConditionalOnMissingBean(HdfsService.class)
    public HdfsService getHdfsService() {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();

        // 如果是HA集群
        if(properties.getHa()) {
            configuration.set("fs.defaultFS", properties.getHaPath());
            configuration.set("dfs.nameservices", properties.getHaClusterAlias());
            configuration.set("dfs.ha.namenodes." + properties.getHaClusterAlias(), properties.getHaNameNodes());
            String rpc="dfs.namenode.rpc-address." + properties.getHaClusterAlias();
            String[] haNns = properties.getHaNameNodes().split(",");
            configuration.set(rpc + "." + haNns[0], properties.getHaRpcAddress1());
            configuration.set(rpc + "." + haNns[1], properties.getHaRpcAddress2());
            configuration.set("dfs.client.failover.proxy.provider."+ properties.getHaClusterAlias(),
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        } else {
            configuration.set("fs.defaultFS", properties.getPath());
        }
        return new HdfsService(configuration);
    }


}
