package com.zhss.microservice.server.slot;

import com.alibaba.fastjson.JSONObject;
import com.zhss.microservice.server.node.network.ServerMessageReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.server.node.persist.FilePersistUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 槽位数据管理组件
 */
public class SlotManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlotManager.class);

    /**
     * 槽位分配存储文件的名字
     */
    private static final String NODE_SLOTS_FILENAME = "node_slots";
    private static final String NODE_SLOTS_REPLICAS_FILENAME = "node_slots_replicas";
    private static final Integer SLOT_COUNT = 16384;

    private SlotManager() {

    }

    static class Singleton {
        static SlotManager instance = new SlotManager();
    }

    public static SlotManager getInstance() {
        return Singleton.instance;
    }

    /**
     * 本节点负责管理的槽位集合
     */
    private Slots slots = new Slots();
    /**
     * 本节点负责管理的槽位副本集合
     */
    private Map<String, SlotsReplica> slotsReplicas = new ConcurrentHashMap<>();

    /**
     * 初始化本节点负责的槽位集合
     */
    public void initSlots(List<String> slotsList) {
        if(slotsList == null) {
            ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
            slotsList = serverMessageReceiver.takeNodeSlots();
        }

        for(String slotScope : slotsList) {
            slots.init(slotScope);
        }
        FilePersistUtils.persist(JSONObject.toJSONString(slotsList).getBytes(), NODE_SLOTS_FILENAME);

        LOGGER.info("初始化本节点槽位数据完毕......");
    }

    /**
     * 初始化本节点负责的槽位副本的集合
     */
    public void initSlotsReplicas(List<String> slotScopes, boolean isController) {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        if(slotScopes == null && !isController) {
            slotScopes = serverMessageReceiver.takeNodeSlotsReplicas();
        } else if(slotScopes == null && isController) {
            return;
        }

        for(String slotScope : slotScopes) {
            SlotsReplica slotsReplica = new SlotsReplica();
            slotsReplica.init(slotScope);
            slotsReplicas.put(slotScope, slotsReplica);
        }

        byte[] bytes = JSONObject.toJSONString(slotScopes).getBytes();
        FilePersistUtils.persist(bytes, NODE_SLOTS_REPLICAS_FILENAME);
        LOGGER.info("初始化本节点槽位副本数据完毕......");
    }

    /**
     * 初始化该节点负责的槽位集合的副本节点id
     */
    public void initReplicaNodeId(Integer replicaNodeId) {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        if(replicaNodeId == null) {
            replicaNodeId = serverMessageReceiver.takeReplicaNodeId();
        }
        slots.setReplicaNodeId(replicaNodeId);
        LOGGER.info("初始化副本节点id完毕：" + replicaNodeId);
    }

    /**
     * 获取槽位
     * @param serviceName
     * @return
     */
    public Slot getSlot(String serviceName) {
        return slots.getSlot(routeSlot(serviceName));
    }

    /**
     * 获取槽位副本
     * @param serviceName
     * @return
     */
    public Slot getSlotReplica(String serviceName) {
        Integer slotNo = routeSlot(serviceName);

        SlotsReplica  slotsReplica = null;

        for(String slotScope : slotsReplicas.keySet()) {
            Integer startSlot = Integer.valueOf(slotScope.split(",")[0]);
            Integer endSlot = Integer.valueOf(slotScope.split(",")[1]);

            if(slotNo >= startSlot && slotNo <= endSlot) {
                slotsReplica = slotsReplicas.get(slotScope);
                break;
            }
        }

        return slotsReplica.getSlot(slotNo);
    }

    /**
     * 将服务路由到slot
     * @param serviceName
     * @return
     */
    private Integer routeSlot(String serviceName) {
        int hashCode = serviceName.hashCode() & Integer.MAX_VALUE;
        Integer slot = hashCode % SLOT_COUNT;

        if(slot == 0) {
            slot = slot + 1;
        }

        return slot;
    }

    public Integer getReplicaNodeId() {
        return slots.getReplicaNodeId();
    }

}
