package com.zhss.microservice.server.node.network;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 远程节点管理组件
 */
public class RemoteServerNodeManager {

    private RemoteServerNodeManager() {

    }

    static class Singleton {
        static RemoteServerNodeManager instance = new RemoteServerNodeManager();
    }

    public static RemoteServerNodeManager getInstance() {
        return Singleton.instance;
    }

    /**
     * 除了controller候选节点以外的其他普通Master节点
     */
    private ConcurrentHashMap<Integer, RemoteServerNode> remoteServerNodes =
            new ConcurrentHashMap<Integer, RemoteServerNode>();

    /**
     * 添加一个远程master节点
     * @param remoteServerNode
     */
    public void addRemoteServerNode(RemoteServerNode remoteServerNode) {
        remoteServerNodes.put(remoteServerNode.getNodeId(), remoteServerNode);
    }

    /**
     * 获取其他的controller候选节点
     * @return
     */
    public List<RemoteServerNode> getOtherControllerCandidates() {
        List<RemoteServerNode> otherControllerCandidates = new ArrayList<RemoteServerNode>();

        for(RemoteServerNode remoteServerNode : remoteServerNodes.values()) {
            if(remoteServerNode.isControllerCandidate()) {
                otherControllerCandidates.add(remoteServerNode);
            }
        }

        return otherControllerCandidates;
    }

    /**
     * 获取所有的远程master节点
     * @return
     */
    public List<RemoteServerNode> getRemoteServerNodes() {
        return new ArrayList<RemoteServerNode>(remoteServerNodes.values());
    }

    /**
     * 删除远程节点
     * @param remoteNodeId
     */
    public void removeServerNode(Integer remoteNodeId) {
        remoteServerNodes.remove(remoteNodeId);
    }

}
