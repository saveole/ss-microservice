package com.zhss.microservice.client.core;

/**
 * 服务实例地址
 */
public class ServiceInstanceAddress {

    private String serviceName;
    private String ip;
    private Integer port;

    public ServiceInstanceAddress(String serviceName, String ip, Integer port) {
        this.serviceName = serviceName;
        this.ip = ip;
        this.port = port;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "ServiceInstanceAddress{" +
                "serviceName='" + serviceName + '\'' +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }
}
