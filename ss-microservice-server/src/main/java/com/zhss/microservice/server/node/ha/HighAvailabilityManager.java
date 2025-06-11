package com.zhss.microservice.server.node.ha;

import com.zhss.microservice.server.node.ControllerCandidate;
import com.zhss.microservice.server.node.ControllerNode;
import com.zhss.microservice.server.node.ServerNodeRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * 高可用管理组件
 */
public class HighAvailabilityManager {

    public HighAvailabilityManager() {
        new Worker().start();
    }

    static class Singleton {
        static HighAvailabilityManager instance = new HighAvailabilityManager();
    }

    public static HighAvailabilityManager getInstance() {
        return Singleton.instance;
    }

    private LinkedBlockingQueue<Integer> disconnectedNodeQueue =
            new LinkedBlockingQueue<>();

    /**
     * 处理跟远程节点网络连接断开的异常
     * @param remoteNodeId
     */
    public void handleDisconnectedException(Integer remoteNodeId) {
        disconnectedNodeQueue.offer(remoteNodeId);
    }

    class Worker extends Thread {

        Logger LOGGER = LoggerFactory.getLogger(Worker.class);

        @Override
        public void run() {
            while(true) {
                try {
                    Integer disconnectedNodeId = disconnectedNodeQueue.take();

                    // 判断是否是跟controller的连接断开
                    // 如果是controller崩溃，以及自己是candidate，则必须启用高可用机制，发起controller重新选举
                    if(ServerNodeRole.isCandidate() &&
                            ControllerNode.isControllerNode(disconnectedNodeId)) {
                        ControllerCandidate controllerCandidate = ControllerCandidate.getInstance();
                        Integer serverNodeRole = controllerCandidate.electController();
                        LOGGER.info("controller重新选举后的结果：" + (serverNodeRole == ServerNodeRole.CONTROLLER ? "Controller" : "Controller候选节点"));

                        if(serverNodeRole.equals(ServerNodeRole.CONTROLLER)) {

                        } else if(serverNodeRole.equals(ServerNodeRole.CANDIDATE)) {

                        }
                    }

                    // 通知原controller的slots槽位副本所在节点，把副本进行转正
                    // 还必须去更新自己的集群元数据，同步集群元数据，此时才可以开放元数据的使用
                } catch(Exception e) {
                    LOGGER.error("高可用机制后台线程运行出错......", e);
                }
            }
        }
    }

}
