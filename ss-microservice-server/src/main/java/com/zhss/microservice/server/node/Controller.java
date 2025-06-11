package com.zhss.microservice.server.node;

import com.alibaba.fastjson.JSONObject;
import com.sun.xml.internal.bind.v2.TODO;
import com.zhss.microservice.server.constant.NodeStatus;
import com.zhss.microservice.server.node.network.RemoteServerNode;
import com.zhss.microservice.server.node.network.RemoteServerNodeManager;
import com.zhss.microservice.server.node.network.ServerNetworkManager;
import com.zhss.microservice.server.slot.SlotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.server.config.Configuration;
import com.zhss.microservice.server.constant.MessageType;
import com.zhss.microservice.server.node.persist.FilePersistUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Master节点之间的Controller
 */
public class Controller {

    private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);

    /**
     * slot槽位的总数量
     */
    private static final int SLOTS_COUNT = 16384;
    /**
     * 槽位分配存储文件的名字
     */
    private static final String SLOTS_ALLOCATION_FILENAME = "slots_allocation";
    /**
     * 槽位分配存储文件的名字
     */
    private static final String SLOTS_REPLICA_ALLOCATION_FILENAME = "slots_replica_allocation";
    private static final String REPLICA_NODE_IDS_FILENAME = "replica_node_ids";
    /**
     * 槽位分配存储文件的名字
     */
    private static final String NODE_SLOTS_FILENAME = "node_slots";
    private static final String NODE_SLOTS_REPLICAS_FILENAME = "node_slots_replicas";

    private Controller() {

    }

    static class Singleton {
        static Controller instance = new Controller();
    }

    public static Controller getInstance() {
        return Singleton.instance;
    }

    /**
     * 槽位分配数据
     */
    private ConcurrentHashMap<Integer, List<String>> slotsAllocation =
            new ConcurrentHashMap<Integer, List<String>>();
    /**
     * 槽位副本分配数据
     */
    private ConcurrentHashMap<Integer, List<String>> slotsReplicaAllocation =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, Integer> replicaNodeIds =
            new ConcurrentHashMap<>();

    /**
     * 分配slot槽位给所有的master机器
     */
    public void allocateSlots() {
        // 计算槽位分配数据
        executeSlotsAllocation();

        // 针对每个节点负责的槽位范围，去把这个槽位范围的副本计算好分配给哪个其他的节点
        // 比如说假设你有4个节点，每个节点都分摊了一个槽位范围
        // 此时针对节点1，他的槽位范围副本就在另外3个节点里随机挑选一个，节点234以此类推
        // 槽位范围副本分配，计算完毕之后，同样在本地磁盘持久化，同步给其他master候选节点
        executeSlotsReplicaAllocation();

        // 将槽位分配数据写入本地磁盘文件
        if(!persistSlotsAllocation()) {
            NodeStatus.fatal();
            return;
        }
        if(!persistSlotsReplicaAllocation()) {
            NodeStatus.fatal();
            return;
        }
        if(!persistReplicaNodeIds()) {
            NodeStatus.fatal();
            return;
        }

        // 槽位已经分配好了，在Controller自己的内存里和磁盘里都持久化了一份
        // Controller要负责把槽位分配数据发送给其他的Controller候选人
        // 其他的Controller候选人就需要在自己内存里维护一份槽位分配数据，以及持久化到磁盘里去
        syncSlotsAllocation();
        syncSlotsReplicaAllocation();
        syncReplicaNodeIds();

        // 在Controller自己内部先进行槽位数据的初始化
        initSlots();
        // 对自己负责的槽位范围副本，在内存里进行初始化，同时也持久化到磁盘上去
        initSlotsReplicas();
        // 把每个节点负责的槽位范围，那个槽位范围的副本在哪个其他节点上，告诉他们
        initReplicaNodeId();

        // Controller除了要把完整的槽位分配数据发送给其他的候选人
        // 他需要给每个master发送一下他们各自对应的槽位范围，各个master在内存里做一个槽位的初始化
        // 还需要在本地磁盘做一个持久化
        sendNodeSlots();
        // 那些节点除了在内存里初始化自己负责的槽位范围
        // 同时还会初始化和持久化自己负责的槽位范围的副本
        sendNodeSlotsReplicas();
        sendReplicaNodeId();

        // 上述都准备完毕之后，后续就可以去开发副本同步机制
        // 高可用机制都做完
        // 来开发一个辅助用的standalone单机模式+持久化机制
    }

    /**
     * 初始化controller节点数据
     */
    public void initControllerNode() {
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();
        ControllerNode.setNodeId(nodeId);
    }

    /**
     * 初始化controller自己的目标槽位副本
     */
    private void initReplicaNodeId() {
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();
        Integer replicaNodeId = replicaNodeIds.get(nodeId);
        SlotManager slotManager = SlotManager.getInstance();
        slotManager.initReplicaNodeId(replicaNodeId);
    }

    /**
     * 对槽位分配数据做一个计算
     */
    private void executeSlotsAllocation() {
        // 获取当前节点的nodeId
        Configuration configuration = Configuration.getInstance();
        Integer myNodeId = configuration.getNodeId();

        // 获取master节点的总数量
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> remoteMasterNodes =
                remoteServerNodeManager.getRemoteServerNodes();
        int totalMasterNodeCount = remoteMasterNodes.size() + 1;
        // 计算平均每个master节点分配到几个slot槽位
        int slotsPerMasterNode = SLOTS_COUNT / totalMasterNodeCount;
        // 计算controller分配到的slot槽位是多少个，如果有多余就加上
        int remainSlotsCount = SLOTS_COUNT - slotsPerMasterNode * totalMasterNodeCount;

        // 初始化每个master节点对应的槽位的数量
        Integer nextStartSlot = 1;
        Integer nextEndSlot = nextStartSlot - 1 + slotsPerMasterNode;

        // 16384 / 3 = 5461
        // 16384 - 5461 * 3 = 1
        // 5462

        // 1~5461
        // 5462~10922
        // 10923~16383

        for(RemoteServerNode remoteMasterNode : remoteMasterNodes) {
            List<String> slotsList = new ArrayList<String>();
            slotsList.add(nextStartSlot + "," + nextEndSlot);
            slotsAllocation.put(remoteMasterNode.getNodeId(), slotsList);

            nextStartSlot = nextEndSlot + 1;
            nextEndSlot = nextStartSlot - 1 + slotsPerMasterNode;
        }

        List<String> slotsList = new ArrayList<String>();
        slotsList.add(nextStartSlot + "," + (nextEndSlot + remainSlotsCount));
        slotsAllocation.put(myNodeId, slotsList);

        LOGGER.info("槽位分配完毕：" + slotsAllocation);
    }

    /**
     * 执行slots副本的分配
     */
    private void executeSlotsReplicaAllocation() {
        // 获取所有节点的node id
        List<Integer> nodeIds = new ArrayList<Integer>();

        Integer myNodeId = Configuration.getInstance().getNodeId();
        nodeIds.add(myNodeId);

        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> remoteMasterNodes =
                remoteServerNodeManager.getRemoteServerNodes();
        for(RemoteServerNode remoteServerNode : remoteMasterNodes) {
            nodeIds.add(remoteServerNode.getNodeId());
        }

        // 执行slots副本的分配
        Random random = new Random();

        for(Map.Entry<Integer, List<String>> nodeSlots : slotsAllocation.entrySet()) {
            Integer nodeId = nodeSlots.getKey();
            List<String> slotsList = nodeSlots.getValue();

            Integer replicaNodeId = null;
            boolean hasDecidedReplicaNode = false;

            while(!hasDecidedReplicaNode) {
                replicaNodeId = nodeIds.get(random.nextInt(nodeIds.size()));
                if(!replicaNodeId.equals(nodeId)) {
                    hasDecidedReplicaNode = true;
                }
            }

            List<String> slotsReplicas = slotsReplicaAllocation.get(replicaNodeId);

            if(slotsReplicas == null) {
                slotsReplicas = new ArrayList<String>();
                slotsReplicaAllocation.put(replicaNodeId, slotsReplicas);
            }
            slotsReplicas.addAll(slotsList);

            replicaNodeIds.put(nodeId, replicaNodeId);
        }

        LOGGER.info("槽位副本分配完毕：" + slotsReplicaAllocation);
    }

    /**
     * 持久化槽位分配数据到本地磁盘
     */
    private Boolean persistSlotsAllocation() {
        String slotsAllocationJSON = JSONObject.toJSONString(slotsAllocation);
        byte[] slotsAllocationByteArray = slotsAllocationJSON.getBytes();
        return FilePersistUtils.persist(slotsAllocationByteArray, SLOTS_ALLOCATION_FILENAME);
    }

    private Boolean persistSlotsReplicaAllocation() {
        String slotsReplicaAllocationJSON = JSONObject.toJSONString(slotsReplicaAllocation);
        byte[] slotsReplicaAllocationByteArray = slotsReplicaAllocationJSON.getBytes();
        return FilePersistUtils.persist(slotsReplicaAllocationByteArray, SLOTS_REPLICA_ALLOCATION_FILENAME);
    }

    private Boolean persistReplicaNodeIds() {
        byte[] bytes = JSONObject.toJSONString(replicaNodeIds).getBytes();
        return FilePersistUtils.persist(bytes, REPLICA_NODE_IDS_FILENAME);
    }

    /**
     * 持久化自己负责的槽位范围到本地磁盘
     */
    private Boolean persistNodeSlots() {
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();
        List<String> slotsList = slotsAllocation.get(nodeId);
        byte[] bytes = JSONObject.toJSONString(slotsList).getBytes();
        return FilePersistUtils.persist(bytes, NODE_SLOTS_FILENAME);
    }

    /**
     * 持久化自己负责的槽位范围副本到本地磁盘
     */
    private Boolean persistNodeSlotsReplicas() {
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();
        List<String> slotsReplicas = slotsReplicaAllocation.get(nodeId);
        byte[] bytes = JSONObject.toJSONString(slotsReplicas).getBytes();
        return FilePersistUtils.persist(bytes, NODE_SLOTS_REPLICAS_FILENAME);
    }

    /**
     * 同步槽位分配数据给其他的controller候选人
     * @return
     */
    private void syncSlotsAllocation() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> otherControllerCandidates =
                remoteServerNodeManager.getOtherControllerCandidates();

        byte[] slotsAllocationByteArray = JSONObject
                .toJSONString(slotsAllocation).getBytes();

        ByteBuffer slotsAllocationByteBuffer =
                ByteBuffer.allocate(4 + slotsAllocationByteArray.length);
        slotsAllocationByteBuffer.putInt(MessageType.SLOTS_ALLOCATION);
        slotsAllocationByteBuffer.put(slotsAllocationByteArray);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode controllerCandidate : otherControllerCandidates) {
            serverNetworkManager.sendMessage(controllerCandidate.getNodeId(), slotsAllocationByteBuffer);
        }

        LOGGER.info("槽位分配数据同步给controller候选节点完毕......");
    }

    private void syncSlotsReplicaAllocation() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> otherControllerCandidates =
                remoteServerNodeManager.getOtherControllerCandidates();

        byte[] bytes = JSONObject.toJSONString(replicaNodeIds).getBytes();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + bytes.length);
        byteBuffer.putInt(MessageType.REPLICA_NODE_IDS);
        byteBuffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode controllerCandidate : otherControllerCandidates) {
            serverNetworkManager.sendMessage(controllerCandidate.getNodeId(), byteBuffer);
        }

        LOGGER.info("副本节点id集合同步给controller候选节点完毕......");
    }

    private void syncReplicaNodeIds() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> otherControllerCandidates =
                remoteServerNodeManager.getOtherControllerCandidates();

        byte[] bytes = JSONObject.toJSONString(slotsReplicaAllocation).getBytes();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + bytes.length);
        byteBuffer.putInt(MessageType.SLOTS_REPLICA_ALLOCATION);
        byteBuffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode controllerCandidate : otherControllerCandidates) {
            serverNetworkManager.sendMessage(controllerCandidate.getNodeId(), byteBuffer);
        }

        LOGGER.info("槽位副本分配数据同步给controller候选节点完毕......");
    }

    /**
     * 对controller自己负责的槽位在内存里进行初始化
     */
    private void initSlots() {
        SlotManager slotManager = SlotManager.getInstance();
        Configuration configuration = Configuration.getInstance();

        Integer nodeId = configuration.getNodeId();
        List<String> slotsList = slotsAllocation.get(nodeId);
        slotManager.initSlots(slotsList);
    }

    /**
     * 对controller自己负责的槽位副本在内存里进行初始化
     */
    private void initSlotsReplicas() {
        SlotManager slotManager = SlotManager.getInstance();
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();
        List<String> slotsReplicas = slotsReplicaAllocation.get(nodeId);
        slotManager.initSlotsReplicas(slotsReplicas, true);
    }

    /**
     * 发送给每个master节点他们负责的槽位范围
     */
    private void sendNodeSlots() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> nodes =
                remoteServerNodeManager.getRemoteServerNodes();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for(RemoteServerNode node : nodes) {
            List<String> slotsList = slotsAllocation.get(node.getNodeId());

            byte[] bytes = JSONObject.toJSONString(slotsList).getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
            buffer.putInt(MessageType.NODE_SLOTS);
            buffer.put(bytes);

            serverNetworkManager.sendMessage(node.getNodeId(), buffer);
        }

        LOGGER.info("给其他节点发送槽位范围完毕......");
    }

    private void sendNodeSlotsReplicas() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> nodes = remoteServerNodeManager.getRemoteServerNodes();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for(RemoteServerNode node : nodes) {
            List<String> slotsReplicas = slotsReplicaAllocation.get(node.getNodeId());
            if(slotsReplicas == null || slotsReplicas.size() == 0) {
                slotsReplicas = new ArrayList<String>();
            }

            byte[] bytes = JSONObject.toJSONString(slotsReplicas).getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
            buffer.putInt(MessageType.NODE_SLOTS_REPLICAS);
            buffer.put(bytes);

        serverNetworkManager.sendMessage(node.getNodeId(), buffer);
        }

        LOGGER.info("给其他节点发送槽位副本完毕......");
    }

    private void sendReplicaNodeId() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> nodes =
                remoteServerNodeManager.getRemoteServerNodes();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for(RemoteServerNode node : nodes) {
            Integer replicaNodeId = replicaNodeIds.get(node.getNodeId());

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
            buffer.putInt(MessageType.REPLICA_NODE_ID);
            buffer.putInt(replicaNodeId);

            serverNetworkManager.sendMessage(node.getNodeId(), buffer);
        }

        LOGGER.info("给其他节点发送副本节点id完毕......");
    }

    public void sendControllerNodeId() {
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();

        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> nodes =
                remoteServerNodeManager.getRemoteServerNodes();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for(RemoteServerNode node : nodes) {
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
            buffer.putInt(MessageType.CONTROLLER_NODE_ID);
            buffer.putInt(nodeId);
            serverNetworkManager.sendMessage(node.getNodeId(), buffer);
        }

        LOGGER.info("给所有节点发送controller节点id完毕......");
    }

    /**
     * 获取slots分配数据
     * @return
     */
    public Map<Integer, List<String>> getSlotsAllocation() {
        return slotsAllocation;
    }

    /**
     * 获取server节点地址列表
     * @return
     */
    public List<String> getServerAddresses() {
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();
        String ip = configuration.getNodeIp();
        Integer clientTcpPort = configuration.getNodeClientTcpPort();

        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> servers = remoteServerNodeManager.getRemoteServerNodes();
        List<String> serverAddresses = new ArrayList<String>();

        for(RemoteServerNode server : servers) {
            serverAddresses.add(server.getNodeId() + ":" + server.getIp() + ":" + server.getClientPort());
        }
        serverAddresses.add(nodeId + ":" + ip + ":" + clientTcpPort);

        return serverAddresses;
    }

}
