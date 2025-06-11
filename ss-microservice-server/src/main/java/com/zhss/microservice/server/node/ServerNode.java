package com.zhss.microservice.server.node;

import com.zhss.microservice.server.node.network.*;
import com.zhss.microservice.server.replica.ReplicationManager;
import com.zhss.microservice.server.slot.SlotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.server.config.Configuration;

/**
 * 微服务平台的server节点
 */
public class ServerNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerNode.class);

    /**
     * 启动server节点
     */
    public void start() {
        Configuration configuration = Configuration.getInstance();
        Boolean isControllerCandidate = configuration.isControllerCandidate();

        // 等待监听其他server节点的连接请求
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.startServerConnectionListener();

        // 如果是controller候选节点
        if(isControllerCandidate) {
            // 主动连接配置文件里排在自己前面的Controller候选节点
            if(!serverNetworkManager.connectBeforeControllerCandidateServers()) {
                return;
            }
            // 等待跟所有其他的server节点都完成连接
            serverNetworkManager.waitAllControllerCandidatesConnected();
            serverNetworkManager.waitAllServerNodeConnected();
        }
        // 如果是普通master节点，就直接对所有的controller候选节点都进行连接
        else {
            serverNetworkManager.connectAllControllerCandidates();
        }

        // 启动server节点的消息接收组件
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        serverMessageReceiver.start();

        // 判断自己是否为controller候选节点
        Boolean isController = false;
        Integer serverNodeRole = ServerNodeRole.COMMON_NODE;

        if(isControllerCandidate) {
            // 投票参加选举controller
            ControllerCandidate controllerCandidate = ControllerCandidate.getInstance();
            serverNodeRole = controllerCandidate.electController();
            LOGGER.info("通过选举得到自己的角色为：" + (serverNodeRole == ServerNodeRole.CONTROLLER ? "Controller" : "Controller候选节点"));

            // 根据自己的角色决定接下来要做什么事情
            if(serverNodeRole == ServerNodeRole.CONTROLLER) {
                isController = true;
                Controller controller = Controller.getInstance();
                controller.allocateSlots();
                controller.initControllerNode();
                controller.sendControllerNodeId();
            } else if(serverNodeRole == ServerNodeRole.CANDIDATE) {
                controllerCandidate.waitForSlotsAllocation();
                controllerCandidate.waitForSlotsReplicaAllocation();
                controllerCandidate.waitReplicaNodeIds();
            }
        }

        // 只要你不是controller，所有的master节点都要在这里等待
        // 等待接收controller分配告诉你的槽位范围
        // 然后最终要做槽位数据的初始化
        if(!isController) {
            SlotManager slotManager = SlotManager.getInstance();
            slotManager.initSlots(null);
            slotManager.initSlotsReplicas(null, false);
            slotManager.initReplicaNodeId(null);
            ControllerNode.setNodeId(serverMessageReceiver.takeControllerNodeId());
        }

        ServerNodeRole.setRole(serverNodeRole);

        // 启动副本复制组件
        ReplicationManager replicationManager = ReplicationManager.getInstance();
        replicationManager.start();

        // 启动master节点的nio服务器
        ClientNIOServer clientNIOServer = ClientNIOServer.getInstance();
        clientNIOServer.start();
    }

}
