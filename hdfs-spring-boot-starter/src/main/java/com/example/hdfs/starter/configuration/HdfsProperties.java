package com.example.hdfs.starter.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author jackie wang
 * @since 2021/3/9 17:24
 */
@ConfigurationProperties(HdfsProperties.PREFIX)
public class HdfsProperties {

    public static final String PREFIX = "spring.hdfs";

    /**
     * hadoop集群地址
     */
    private String path;

    /**
     * hadoop集群用户
     */
    private String username;

    /**
     * 是否HA集群，默认false
     */
    private Boolean isHa = false;

    /**
     * hadoop HA集群地址
     */
    private String haPath;

    /**
     * HA集群别名
     */
    private String haClusterAlias;

    /**
     * HA集群namenode列表
     */
    private String haNameNodes;

    /**
     * ha集群的namenode1的RPC通讯地址
     */
    private String haRpcAddress1;

    /**
     * ha集群的namenode2的RPC通讯地址
     */
    private String haRpcAddress2;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Boolean getHa() {
        return isHa;
    }

    public void setHa(Boolean ha) {
        isHa = ha;
    }

    public String getHaPath() {
        return haPath;
    }

    public void setHaPath(String haPath) {
        this.haPath = haPath;
    }

    public String getHaClusterAlias() {
        return haClusterAlias;
    }

    public void setHaClusterAlias(String haClusterAlias) {
        this.haClusterAlias = haClusterAlias;
    }

    public String getHaNameNodes() {
        return haNameNodes;
    }

    public void setHaNameNodes(String haNameNodes) {
        this.haNameNodes = haNameNodes;
    }

    public String getHaRpcAddress1() {
        return haRpcAddress1;
    }

    public void setHaRpcAddress1(String haRpcAddress1) {
        this.haRpcAddress1 = haRpcAddress1;
    }

    public String getHaRpcAddress2() {
        return haRpcAddress2;
    }

    public void setHaRpcAddress2(String haRpcAddress2) {
        this.haRpcAddress2 = haRpcAddress2;
    }


}
