package com.zhss.microservice.server.node.network;

import com.alibaba.fastjson.JSONObject;
import com.zhss.microservice.common.entity.HeartbeatRequest;
import com.zhss.microservice.common.entity.RegisterRequest;
import com.zhss.microservice.common.entity.Request;
import com.zhss.microservice.server.constant.NodeStatus;
import com.zhss.microservice.server.node.ControllerVote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.server.constant.MessageType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * master节点的消息接收组件（线程）
 *
 * 1、不停的从网络通信组件的receiver队列里获取最新的消息
 * 2、判断消息的类型，对消息进行对象转化
 * 3、把消息推送到不同的业务模块所对应的队列里去
 * 4、对各种不同的业务模块提供获取自己业务的消息的接口
 *
 */
public class ServerMessageReceiver extends Thread {

    static final Logger LOGGER = LoggerFactory.getLogger(ServerMessageReceiver.class);


    private ServerMessageReceiver() {

    }

    static class Singleton {
        static ServerMessageReceiver instance = new ServerMessageReceiver();
    }

    public static ServerMessageReceiver getInstance() {
        return Singleton.instance;
    }

    /**
     * 投票消息接收队列
     */
    private LinkedBlockingQueue<ControllerVote> voteReceiveQueue =
            new LinkedBlockingQueue<ControllerVote>();
    /**
     * 槽位数据接收队列
     */
    private LinkedBlockingQueue<ConcurrentHashMap<Integer, List<String>>> slotsAllocationReceiveQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ConcurrentHashMap<Integer, List<String>>> slotsReplicaAllocationReceiveQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ConcurrentHashMap<Integer, Integer>> replicaNodeIdsQueue =
            new LinkedBlockingQueue<>();
    /**
     * 自己负责的槽位范围的消息接收队列
     */
    private LinkedBlockingQueue<List<String>> nodeSlotsQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<List<String>> nodeSlotsReplicasQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Integer> replicaNodeIdQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Integer> controllerNodeIdQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Request> replicaRequestQueue =
            new LinkedBlockingQueue<>();

    /**
     * 线程主循环体
     */
    public void run() {
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();

        while(NodeStatus.isRunning()) {
            try {
                ByteBuffer message = serverNetworkManager.takeMessage();
                int messageType = message.getInt();

                // 如果是controller投票消息
                if (messageType == MessageType.VOTE) {
                    ControllerVote vote = new ControllerVote(message);
                    voteReceiveQueue.put(vote);
                    LOGGER.info("收到一个controller投票: " + vote);
                }
                // 如果是槽位分配消息
                else if (messageType == MessageType.SLOTS_ALLOCATION) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    String slotsAllocationJSON = new String(bytes);
                    ConcurrentHashMap<Integer, List<String>> slotsAllocation = JSONObject.parseObject(
                            slotsAllocationJSON, ConcurrentHashMap.class);
                    slotsAllocationReceiveQueue.put(slotsAllocation);

                    LOGGER.info("收到槽位分配数据: " + slotsAllocation);
                }
                // 如果是负责槽位范围消息
                else if(messageType == MessageType.NODE_SLOTS) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    String slotsListJSON = new String(bytes);
                    List<String> slotsList = JSONObject.parseObject(slotsListJSON, ArrayList.class);
                    nodeSlotsQueue.put(slotsList);

                    LOGGER.info("收到本节点负责的槽位范围: " + slotsList);
                }
                else if(messageType == MessageType.SLOTS_REPLICA_ALLOCATION) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    String slotsReplicaAllocationJson = new String(bytes);
                    ConcurrentHashMap<Integer, List<String>> slotsReplicaAllocation = JSONObject.parseObject(
                            slotsReplicaAllocationJson, ConcurrentHashMap.class);
                    slotsReplicaAllocationReceiveQueue.put(slotsReplicaAllocation);

                    LOGGER.info("收到槽位副本分配数据: " + slotsReplicaAllocation);
                } else if(messageType == MessageType.NODE_SLOTS_REPLICAS) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    List<String> slotsReplicas = JSONObject.parseObject(new String(bytes), List.class);
                    nodeSlotsReplicasQueue.put(slotsReplicas);

                    LOGGER.info("收到本节点负责的槽位副本集合: " + slotsReplicas);
                } else if(messageType == MessageType.REPLICA_NODE_ID) {
                    Integer replicaNodeId = message.getInt();
                    replicaNodeIdQueue.put(replicaNodeId);
                    LOGGER.info("收到副本节点id: " + replicaNodeId);
                } else if(messageType == MessageType.REPLICA_REGISTER) {
                    Integer messageFlag = message.getInt();
                    Integer messageBodyLength = message.getInt();
                    Integer requestType = message.getInt();

                    RegisterRequest registerRequest =
                            RegisterRequest.deserialize(message);

                    replicaRequestQueue.put(registerRequest);

                    LOGGER.info("收到给副本转发的服务注册请求: " + registerRequest);
                } else if(messageType == MessageType.REPLICA_HEARTBEAT) {
                    Integer messageFlag = message.getInt();
                    Integer messageBodyLength = message.getInt();
                    Integer requestType = message.getInt();

                    HeartbeatRequest heartbeatRequest =
                            HeartbeatRequest.deserialize(message);

                    replicaRequestQueue.put(heartbeatRequest);

                    LOGGER.info("收到给副本转发的服务心跳请求: " + heartbeatRequest);
                }
                else if(messageType == MessageType.REPLICA_NODE_IDS) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    ConcurrentHashMap<Integer, Integer> replicaNodeIds = JSONObject.parseObject(
                            new String(bytes), ConcurrentHashMap.class);
                    replicaNodeIdsQueue.put(replicaNodeIds);

                    LOGGER.info("收到副本节点id集合: " + replicaNodeIds);
                } else if(messageType == MessageType.CONTROLLER_NODE_ID) {
                    Integer controllerNodeId = message.getInt();
                    controllerNodeIdQueue.put(controllerNodeId);
                    LOGGER.info("收到controller节点id: " + controllerNodeId);
                }
            } catch(Exception e) {
                LOGGER.error("receive message error......", e);
            }
        }
    }

    /**
     * 阻塞式获取投票消息
     * @return
     */
    public ControllerVote takeVote() {
        try {
            return voteReceiveQueue.take();
        } catch(Exception e) {
            LOGGER.error("take vote message error......", e);
            return null;
        }
    }

    /**
     * 阻塞式获取槽位分配消息
     * @return
     */
    public ConcurrentHashMap<Integer, List<String>> takeSlotsAllocation() {
        try {
            return slotsAllocationReceiveQueue.take();
        } catch(Exception e) {
            LOGGER.error("take slots allocation message error......", e);
            return null;
        }
    }

    public ConcurrentHashMap<Integer, List<String>> takeSlotsReplicaAllocation() {
        try {
            return slotsReplicaAllocationReceiveQueue.take();
        } catch(Exception e) {
            LOGGER.error("take slots allocation message error......", e);
            return null;
        }
    }

    /**
     * 阻塞式获取负责的槽位范围
     * @return
     */
    public List<String> takeNodeSlots() {
        try {
            return nodeSlotsQueue.take();
        } catch(Exception e) {
            LOGGER.error("take node slots message error......", e);
            return null;
        }
    }

    public List<String> takeNodeSlotsReplicas() {
        try {
            return nodeSlotsReplicasQueue.take();
        } catch(Exception e) {
            LOGGER.error("take node slots message error......", e);
            return null;
        }
    }

    public Integer takeReplicaNodeId() {
        try {
            return replicaNodeIdQueue.take();
        } catch(Exception e) {
            LOGGER.error("take node slots message error......", e);
            return null;
        }
    }

    public Request takeReplicaRequest() {
        try {
            return replicaRequestQueue.take();
        } catch(Exception e) {
            LOGGER.error("获取副本转发请求失败！", e);
            return null;
        }
    }

    public ConcurrentHashMap<Integer, Integer> takeReplicaNodeIds() {
        try {
            return replicaNodeIdsQueue.take();
        } catch(Exception e) {
            LOGGER.error("获取副本节点id集合失败！", e);
            return null;
        }
    }

    public Integer takeControllerNodeId() {
        try {
            return controllerNodeIdQueue.take();
        } catch(Exception e) {
            LOGGER.error("获取controller节点id失败！", e);
            return null;
        }
    }

}
