package com.zhss.microservice.server.slot;

import com.zhss.microservice.server.slot.registry.ServiceRegistry;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 本节点负责的slot槽位的集合
 */
public class Slots {

    /**
     * 节点本地内存里的槽位
     */
    private ConcurrentHashMap<Integer, Slot> slots =
            new ConcurrentHashMap<>();
    /**
     * 该槽位集合的副本节点的id
     */
    private Integer replicaNodeId;

    /**
     * 对槽位集合进行初始化
     * @param slotScope
     */
    public void init(String slotScope) {
        String[] slotScopeSplited = slotScope.split(",");
        Integer startSlotNo = Integer.valueOf(slotScopeSplited[0]);
        Integer endSlotNo = Integer.valueOf(slotScopeSplited[1]);

        ServiceRegistry serviceRegistry = new ServiceRegistry(false);

        for(Integer slotNo = startSlotNo; slotNo <= endSlotNo; slotNo++) {
            slots.put(slotNo, new Slot(slotNo, serviceRegistry));
        }
    }

    /**
     * 设置该槽位集合的副本节点id
     * @param replicaNodeId
     */
    public void setReplicaNodeId(Integer replicaNodeId) {
        this.replicaNodeId = replicaNodeId;
    }

    public Slot getSlot(Integer slotNo) {
        return slots.get(slotNo);
    }

    public Integer getReplicaNodeId() {
        return replicaNodeId;
    }

}
