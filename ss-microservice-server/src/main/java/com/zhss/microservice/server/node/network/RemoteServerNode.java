package com.zhss.microservice.server.node.network;

/**
 * 远程master节点的数据
 */
public class RemoteServerNode {

    /**
     * 节点id
     */
    private Integer nodeId;
    /**
     * 是否为controller候选节点
     */
    private Boolean isControllerCandidate;
    /**
     * 节点的ip地址
     */
    private String ip;
    /**
     * 面向客户端的端口号
     */
    private Integer clientPort;

    public RemoteServerNode(Integer nodeId,
                            Boolean isControllerCandidate,
                            String ip,
                            Integer clientPort) {
        this.nodeId = nodeId;
        this.isControllerCandidate = isControllerCandidate;
        this.ip = ip;
        this.clientPort = clientPort;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    public Boolean isControllerCandidate() {
        return isControllerCandidate;
    }

    public void setControllerCandidate(Boolean controllerCandidate) {
        isControllerCandidate = controllerCandidate;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getClientPort() {
        return clientPort;
    }

    public void setClientPort(Integer clientPort) {
        this.clientPort = clientPort;
    }

    @Override
    public String toString() {
        return "RemoteMasterNode{" +
                "nodeId=" + nodeId +
                ", isControllerCandidate=" + isControllerCandidate +
                ", ip='" + ip + '\'' +
                ", clientPort=" + clientPort +
                '}';
    }

}
