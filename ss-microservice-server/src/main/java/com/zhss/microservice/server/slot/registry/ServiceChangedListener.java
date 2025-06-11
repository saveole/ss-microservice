package com.zhss.microservice.server.slot.registry;

import com.zhss.microservice.common.entity.Request;
import com.zhss.microservice.common.entity.ServiceChangedRequest;
import com.zhss.microservice.server.node.network.ClientMessageQueues;

import java.util.ArrayList;
import java.util.List;

/**
 * 服务变动监听器
 */
public class ServiceChangedListener {

    /**
     * 客户端连接标识
     */
    private String clientConnectionId;

    public ServiceChangedListener(String clientConnectionId) {
        this.clientConnectionId = clientConnectionId;
    }

    /**
     * 回调事件
     * @param serviceInstances
     */
    public void onChange(String serviceName, List<ServiceInstance> serviceInstances) {
        List<String> serviceInstanceAddresses = new ArrayList<String>();
        for(ServiceInstance serviceInstance : serviceInstances) {
            serviceInstanceAddresses.add(serviceInstance.getAddress());
        }

        // 构建一个反向推送服务实例地址列表变动的请求
        Request request = new ServiceChangedRequest.Builder()
                .serviceName(serviceName)
                .serviceInstanceAddresses(serviceInstanceAddresses)
                .build();

        // 把这个请求推送到客户端连接的请求队列里去
        ClientMessageQueues clientRequestQueues = ClientMessageQueues.getInstance();
        clientRequestQueues.offerMessage(clientConnectionId, request);
    }

}
