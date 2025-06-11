package com.zhss.microservice.server.constant;

/**
 * 服务治理平台的节点状态
 */
public class NodeStatus {

    /**
     * 正在初始化
     */
    public static final Integer INITIALIZING = 0;
    /**
     * 正在运行中
     */
    public static final Integer RUNNING = 1;
    /**
     * 已经关闭了
     */
    public static final Integer SHUTDOWN = 2;
    /**
     * 系统崩溃了
     */
    public static final Integer FATAL = 3;

    private NodeStatus() {

    }

    private static class Singleton {

        static NodeStatus instance = new NodeStatus();

    }

    public static NodeStatus getInstance() {
        return Singleton.instance;
    }

    /**
     * 节点状态
     */
    private volatile int status;

    /**
     * 设置节点状态
     * @param status 节点状态
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * 获取节点状态
     * @return 节点状态
     */
    public int getStatus() {
        return status;
    }

    /**
     * 获取节点状态
     * @return 节点状态
     */
    public static int get() {
        return getInstance().getStatus();
    }

    /**
     * 判断当前节点是否处于运行状态
     * @return
     */
    public static boolean isRunning() {
        return get() == NodeStatus.RUNNING;
    }

    /**
     * 标识系统状态为崩溃
     */
    public static void fatal() {
        NodeStatus nodeStatus = getInstance();
        nodeStatus.setStatus(NodeStatus.FATAL);
    }

    /**
     * 系统状态是否崩溃了
     * @return
     */
    public static boolean isFatal() {
        return get() == NodeStatus.FATAL;
    }

}
