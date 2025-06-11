package com.zhss.microservice.server.slot;

import com.zhss.microservice.server.slot.registry.ServiceRegistry;

/**
 * 槽位
 */
public class Slot {

    /**
     * 槽位编号
     */
    private Integer slotNo;
    /**
     * 服务注册表
     */
    private ServiceRegistry serviceRegistry;

    /**
     * 构造函数
     * @param slotNo
     */
    public Slot(Integer slotNo, ServiceRegistry serviceRegistry) {
        this.slotNo = slotNo;
        // TODO: 不能是单例的，必须是独立的数据实例
        this.serviceRegistry = serviceRegistry;
    }

    /**
     * 获取服务注册中心数据分片实例
     * @return
     */
    public ServiceRegistry getServiceRegistry() {
        return this.serviceRegistry;
    }

}
