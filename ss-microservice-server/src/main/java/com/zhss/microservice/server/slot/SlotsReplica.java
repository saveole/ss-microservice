package com.zhss.microservice.server.slot;

import com.zhss.microservice.server.slot.registry.ServiceRegistry;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 本节点负责的槽位副本
 */
public class SlotsReplica {

    /**
     * 槽位副本里的槽位
     */
    private ConcurrentHashMap<Integer, Slot> slots =
            new ConcurrentHashMap<>();

    public void init(String slotScope) {
        String[] slotScopeSplited = slotScope.split(",");
        Integer startSlotNo = Integer.valueOf(slotScopeSplited[0]);
        Integer endSlotNo = Integer.valueOf(slotScopeSplited[1]);

        ServiceRegistry serviceRegistry = new ServiceRegistry(true);

        for(Integer slotNo = startSlotNo; slotNo <= endSlotNo; slotNo++) {
            slots.put(slotNo, new Slot(slotNo, serviceRegistry));
        }
    }

    public Slot getSlot(Integer slotNo) {
        return slots.get(slotNo);
    }

}
