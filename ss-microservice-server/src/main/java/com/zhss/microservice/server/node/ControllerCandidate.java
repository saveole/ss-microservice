package com.zhss.microservice.server.node;

import com.alibaba.fastjson.JSONObject;
import com.zhss.microservice.server.constant.NodeStatus;
import com.zhss.microservice.server.node.network.RemoteServerNode;
import com.zhss.microservice.server.node.network.RemoteServerNodeManager;
import com.zhss.microservice.server.node.network.ServerMessageReceiver;
import com.zhss.microservice.server.node.network.ServerNetworkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.server.config.Configuration;
import com.zhss.microservice.server.node.persist.FilePersistUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Controller候选人
 */
public class ControllerCandidate {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerCandidate.class);

    /**
     * 槽位分配存储文件的名字
     */
    private static final String SLOTS_ALLOCATION_FILENAME = "slot_allocation";
    private static final String SLOTS_REPLICA_ALLOCATION_FILENAME = "slot_replica_allocation";
    private static final String REPLICA_NODE_IDS_FILENAME = "replica_node_ids";
    /**
     * 等待所有master节点连接过来的检查间隔
     */
    private static final Long ALL_MASTER_NODE_CONNECT_CHECK_INTERVAL = 100L;

    private ControllerCandidate() {

    }

    static class Singleton {
        static ControllerCandidate instance = new ControllerCandidate();
    }

    public static ControllerCandidate getInstance() {
        return Singleton.instance;
    }

    /**
     * 投票轮次
     */
    private int voteRound = 1;
    /**
     * 当前的一个投票
     */
    private ControllerVote currentVote;
    /**
     * 槽位分配数据
     */
    private ConcurrentHashMap<Integer, List<String>> slotsAllocation;
    private ConcurrentHashMap<Integer, List<String>> slotsReplicaAllocation;
    private ConcurrentHashMap<Integer, Integer> replicaNodeIds;

    /**
     * 发起controller选举
     * @return
     */
    public int electController() {
        // 定义自己的节点id
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();

        // 获取到其他所有的controller候选节点
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> otherControllerCandidates =
                remoteServerNodeManager.getOtherControllerCandidates();
        LOGGER.info("其他的Controller候选节点包括: " + otherControllerCandidates);

        // 初始化好自己第一轮投票的选票
        this.voteRound = 1;
        this.currentVote = new ControllerVote(nodeId, nodeId, voteRound);

        // 开启新一轮的投票
        Integer controllerNodeId = startNextRoundVote(otherControllerCandidates);
        // 如果第一轮投票没有找出谁是controller
        while(controllerNodeId == null) {
            controllerNodeId = startNextRoundVote(otherControllerCandidates);
        }

        // 通过投票找到谁是controller了
        if(nodeId.equals(controllerNodeId)) {
            return ServerNodeRole.CONTROLLER;
        } else {
            return ServerNodeRole.CANDIDATE;
        }
    }

    /**
     * 开启下一轮投票
     */
    private Integer startNextRoundVote(
            List<RemoteServerNode> otherControllerCandidates) {
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();

        LOGGER.info("开始第" + voteRound + "几轮投票......");

        // 定义自己的节点id
        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();

        // 定义quorum的数量，比如controller候选节点有3个，quorum = 3 / 2 + 1 = 2
        int candidateCount = (1 + otherControllerCandidates.size());
        int quorum = candidateCount / 2 + 1;

        // 定义一个归票的集合
        List<ControllerVote> votes = new ArrayList<ControllerVote>();
        votes.add(currentVote);

        // 发送初始的选票（都是投给自己的），给其他的候选节点
        ByteBuffer voteMessage = currentVote.getMessageByteBuffer();
        for(RemoteServerNode remoteNode : otherControllerCandidates) {
            Integer remoteNodeId = remoteNode.getNodeId();
            serverNetworkManager.sendMessage(remoteNodeId, voteMessage);
            LOGGER.info("发送Controller选举投票给server节点: " + remoteNode);
        }

        // 在当前这一轮投票里，开始等待别人的选票
        while(NodeStatus.isRunning()) {
            ControllerVote receivedVote = serverMessageReceiver.takeVote();

            if(receivedVote.getVoterNodeId() == null) {
                continue;
            }

            // 对收到的选票进行归票
            votes.add(receivedVote);
            LOGGER.info("从server节点接收到Controller选举投票: " + receivedVote);

            // 如果发现总票数大于等于了quorum的数量，此时可以进行判定
            if(votes.size() >= quorum) {
                Integer judgedControllerNodeId =
                        getControllerFromVotes(votes, quorum);

                // 如果判断出来有人是controller了
                if(judgedControllerNodeId != null) {
                    if(votes.size() == candidateCount) {
                        LOGGER.info("确认谁是Controller: " + judgedControllerNodeId + ", 已经收到所有的投票......");
                        return judgedControllerNodeId;
                    }
                    LOGGER.info("确认谁是Controller: " + judgedControllerNodeId + ", 但是还没有收到所有的投票......");
                } else {
                    LOGGER.info("还无法确认谁是Controller: " + votes);
                }
            }

            if(votes.size() == candidateCount) {
                // 所有候选人的选票都收到了，此时还没决定出controller
                // 这一轮选举就失败了
                // 调整自己下一轮的投票给谁，找到当前候选人里id最大的那个
                voteRound++;
                Integer betterControllerNodeId = getBetterControllerNodeId(votes);
                this.currentVote = new ControllerVote(nodeId, betterControllerNodeId, voteRound);

                LOGGER.info("本轮投票失败, 尝试创建更好的一个投票: " + currentVote);

                return null;
            }
        }

        return null;
    }

    /**
     * 依据现有的选票获取到一个controller节点的id
     * @param votes
     * @return
     */
    private Integer getControllerFromVotes(List<ControllerVote> votes, int quorum) {
        // <1, 1>, <2, 1>, <3, 2>
        Map<Integer, Integer> voteCountMap = new HashMap<Integer, Integer>();

        for(ControllerVote vote : votes) {
            Integer controllerNodeId = vote.getControllerNodeId();

            Integer count = voteCountMap.get(controllerNodeId);
            if(count == null) {
                count = 0;
            }

            voteCountMap.put(controllerNodeId, ++count);
        }

        for(Map.Entry<Integer, Integer> voteCountEntry : voteCountMap.entrySet()) {
            if(voteCountEntry.getValue() >= quorum) {
                return voteCountEntry.getKey();
            }
        }

        return null;
    }

    /**
     * 从选票里获取最大的那个controller节点id
     * @param votes
     * @return
     */
    private Integer getBetterControllerNodeId(List<ControllerVote> votes) {
        Integer betterControllerNodeId = 0;

        for(ControllerVote vote : votes) {
            Integer controllerNodeId = vote.getControllerNodeId();
            if(controllerNodeId > betterControllerNodeId) {
                betterControllerNodeId = controllerNodeId;
            }
        }

        return betterControllerNodeId;
    }

    /**
     * 阻塞等待槽位分配数据
     */
    public void waitForSlotsAllocation() {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        this.slotsAllocation = serverMessageReceiver.takeSlotsAllocation();
        String slotsAllocationJSON = JSONObject.toJSONString(slotsAllocation);
        byte[] slotsAllocationByteArray = slotsAllocationJSON.getBytes();
        FilePersistUtils.persist(slotsAllocationByteArray, SLOTS_ALLOCATION_FILENAME);
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
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();

        Configuration configuration = Configuration.getInstance();
        Integer nodeId = configuration.getNodeId();
        String ip = configuration.getNodeIp();
        Integer clientTcpPort = configuration.getNodeClientTcpPort();

        List<RemoteServerNode> servers = remoteServerNodeManager.getRemoteServerNodes();
        List<String> serverAddresses = new ArrayList<String>();

        for(RemoteServerNode server : servers) {
            serverAddresses.add(server.getNodeId() + ":" + server.getIp() + ":" + server.getClientPort());
        }
        serverAddresses.add(nodeId + ":" + ip + ":" + clientTcpPort);

        return serverAddresses;
    }

    public void waitForSlotsReplicaAllocation() {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        this.slotsReplicaAllocation = serverMessageReceiver.takeSlotsReplicaAllocation();
        String slotsReplicaAllocationJSON = JSONObject.toJSONString(slotsReplicaAllocation);
        byte[] slotsReplicaAllocationByteArray = slotsReplicaAllocationJSON.getBytes();
        FilePersistUtils.persist(slotsReplicaAllocationByteArray, SLOTS_REPLICA_ALLOCATION_FILENAME);
    }

    public void waitReplicaNodeIds() {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        this.replicaNodeIds = serverMessageReceiver.takeReplicaNodeIds();
        byte[] bytes = JSONObject.toJSONString(replicaNodeIds).getBytes();
        FilePersistUtils.persist(bytes, REPLICA_NODE_IDS_FILENAME);
    }

}
