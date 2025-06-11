package com.zhss.microservice.server.constant;

/**
 * master节点之间的消息类型
 */
public class MessageType {

    /**
     * 终止运行消息
     */
    public static final Integer TERMINATE = -1;
    /**
     * 投票消息类型
     */
    public static final Integer VOTE = 1;
    /**
     * 槽位分配消息类型
     */
    public static final Integer SLOTS_ALLOCATION = 2;
    /**
     * 节点负责的槽位范围
     */
    public static final Integer NODE_SLOTS = 3;
    /**
     * 槽位副本分配消息类型
     */
    public static final Integer SLOTS_REPLICA_ALLOCATION = 4;
    /**
     * 节点负责的槽位副本
     */
    public static final Integer NODE_SLOTS_REPLICAS = 5;
    /**
     * 副本节点id
     */
    public static final Integer REPLICA_NODE_ID = 6;
    /**
     * 注册请求转发副本
     */
    public static final Integer REPLICA_REGISTER = 7;
    /**
     * 心跳请求转发副本
     */
    public static final Integer REPLICA_HEARTBEAT = 8;
    /**
     * 副本节点id集合
     */
    public static final Integer REPLICA_NODE_IDS = 9;
    /**
     * 副本节点id
     */
    public static final Integer CONTROLLER_NODE_ID = 10;

}
