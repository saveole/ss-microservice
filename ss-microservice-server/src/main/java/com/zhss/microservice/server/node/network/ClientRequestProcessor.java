package com.zhss.microservice.server.node.network;

import com.zhss.microservice.common.entity.*;
import com.zhss.microservice.server.config.Configuration;
import com.zhss.microservice.server.constant.MessageType;
import com.zhss.microservice.server.node.Controller;
import com.zhss.microservice.server.node.ControllerCandidate;
import com.zhss.microservice.server.node.ServerNodeRole;
import com.zhss.microservice.server.slot.Slot;
import com.zhss.microservice.server.slot.SlotManager;
import com.zhss.microservice.server.slot.registry.ServiceInstance;
import com.zhss.microservice.server.slot.registry.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 客户端请求处理组件
 */
public class ClientRequestProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientRequestProcessor.class);

    private ClientRequestProcessor() {

    }

    static class Singleton {
        static ClientRequestProcessor instance = new ClientRequestProcessor();
    }

    public static ClientRequestProcessor getInstance() {
        return Singleton.instance;
    }

    /**
     * 处理客户端请求
     * @param request
     */
    public Response process(String clientConnectionId, Request request) {
        if(request instanceof FetchSlotsAllocationRequest) {
            return fetchSlotsAllocation((FetchSlotsAllocationRequest) request);
        } else if(request instanceof FetchServerAddressesRequest) {
            return fetchServerAddresses((FetchServerAddressesRequest) request);
        } else if(request instanceof RegisterRequest) {
            return register((RegisterRequest) request);
        } else if(request instanceof  HeartbeatRequest) {
            return heartbeat((HeartbeatRequest) request);
        } else if(request instanceof FetchServerNodeIdRequest) {
            return fetchServerNodeId((FetchServerNodeIdRequest)request);
        } else if(request instanceof  SubscribeRequest) {
            return subscribe(clientConnectionId, (SubscribeRequest)request);
        }

        return null;
    }

    /**
     * 服务订阅
     * @param request
     * @return
     */
    private Response subscribe(String clientConnectionId, SubscribeRequest request) {
        String serviceName = request.getServiceName();

        SlotManager slotManager = SlotManager.getInstance();
        Slot slot = slotManager.getSlot(serviceName);
        ServiceRegistry serviceRegistry = slot.getServiceRegistry();

        // 设计模式：门面模式
        // 不同的槽位slot，就负责一部分请求的处理，逻辑单元
        List<ServiceInstance> serviceInstances = serviceRegistry.subscribe(
                clientConnectionId, serviceName);

        List<String> serviceInstanceAddresses = new ArrayList<String>();

        for(ServiceInstance serviceInstance : serviceInstances) {
            serviceInstanceAddresses.add(
                    serviceInstance.getServiceName() + "," +
                    serviceInstance.getServiceInstanceIp() + "," +
                    serviceInstance.getServiceInstancePort()
            );
        }

        LOGGER.info("客户端【" + clientConnectionId + "】订阅服务【" + serviceName + "】：" + serviceInstanceAddresses);

        SubscribeResponse response = new SubscribeResponse.Builder()
                .requestId(request.getId())
                .serviceInstanceAddresses(serviceInstanceAddresses)
                .build();

        return response;
    }

    /**
     * 获取server节点id
     * @param request
     * @return
     */
    private Response fetchServerNodeId(FetchServerNodeIdRequest request) {
        Configuration configuration = Configuration.getInstance();
        Integer serverNodeId = configuration.getNodeId();

        FetchServerNodeIdResponse response = new FetchServerNodeIdResponse.Builder()
                .requestId(request.getId())
                .serverNodeId(serverNodeId)
                .build();

        return response;
    }

    /**
     * 心跳
     * @param request
     * @return
     */
    private Response heartbeat(HeartbeatRequest request) {
        // 提取服务实例信息
        String serviceName = request.getServiceName();
        String serviceInstanceIp = request.getServiceInstanceIp();
        Integer serviceInstancePort = request.getServiceInstancePort();

        // 在本节点完成服务实例的心跳
        SlotManager slotManager = SlotManager.getInstance();
        Slot slot = slotManager.getSlot(serviceName);
        ServiceRegistry serviceRegistry = slot.getServiceRegistry();
        serviceRegistry.heartbeat(serviceName, serviceInstanceIp, serviceInstancePort);

        // 就要获取到你的副本节点id，转发注册请求过去给他
        Integer replicaNodeId = slotManager.getReplicaNodeId();

        // 构造副本注册请求，转发给那个指定的node
        HeartbeatRequest replicaRequest = new HeartbeatRequest.Builder()
                .serviceName(request.getServiceName())
                .serviceInstanceIp(request.getServiceInstanceIp())
                .serviceInstancePort(request.getServiceInstancePort())
                .build();
        byte[] replicaRequestBytes = replicaRequest.getData().array();

        ByteBuffer replicaRequestBuffer = ByteBuffer.allocate(4 + replicaRequestBytes.length);
        replicaRequestBuffer.putInt(MessageType.REPLICA_HEARTBEAT);
        replicaRequestBuffer.put(replicaRequestBytes);

        // 那个副本节点，接收到了副本注册请求，此时就会找到slot在自己节点上的副本，完成注册
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(replicaNodeId, replicaRequestBuffer);

        // 返回响应给服务实例
        HeartbeatResponse.Builder builder = new HeartbeatResponse.Builder();
        HeartbeatResponse response = builder
                .requestId(request.getId())
                .result(RegisterResponse.SUCCESS)
                .build();

        return response;
    }

    /**
     * 服务注册
     * @param request
     * @return
     */
    private Response register(RegisterRequest request) {
        // 封装服务实例
        String serviceName = request.getServiceName();
        String serviceInstanceIp = request.getServiceInstanceIp();
        Integer serviceInstancePort = request.getServiceInstancePort();

        ServiceInstance serviceInstance = new ServiceInstance(
                serviceName,
                serviceInstanceIp,
                serviceInstancePort
        );

        // 在本节点完成服务注册的功能
        SlotManager slotManager = SlotManager.getInstance();
        Slot slot = slotManager.getSlot(serviceName);
        ServiceRegistry serviceRegistry = slot.getServiceRegistry();
        serviceRegistry.register(serviceInstance);
        LOGGER.info("完成服务实例【" + serviceInstance + "】的注册......");

        // 就要获取到你的副本节点id，转发注册请求过去给他
        Integer replicaNodeId = slotManager.getReplicaNodeId();

        // 构造副本注册请求，转发给那个指定的node
        RegisterRequest replicaRequest = new RegisterRequest.Builder()
                .serviceName(request.getServiceName())
                .serviceInstanceIp(request.getServiceInstanceIp())
                .serviceInstancePort(request.getServiceInstancePort())
                .build();
        byte[] replicaRequestBytes = replicaRequest.getData().array();

        ByteBuffer replicaRequestBuffer = ByteBuffer.allocate(4 + replicaRequestBytes.length);
        replicaRequestBuffer.putInt(MessageType.REPLICA_REGISTER);
        replicaRequestBuffer.put(replicaRequestBytes);

        // 那个副本节点，接收到了副本注册请求，此时就会找到slot在自己节点上的副本，完成注册
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(replicaNodeId, replicaRequestBuffer);

        RegisterResponse.Builder builder = new RegisterResponse.Builder();
        RegisterResponse response = builder
                .requestId(request.getId())
                .result(RegisterResponse.SUCCESS)
                .build();

        return response;
    }

    /**
     * 抓取slots分配数据
     * @param fetchSlotsAllocationRequest
     */
    private FetchSlotsAllocationResponse fetchSlotsAllocation(
            FetchSlotsAllocationRequest fetchSlotsAllocationRequest) {
        String requestId = fetchSlotsAllocationRequest.getId();

        Map<Integer, List<String>> slotsAllocation = null;

        if(ServerNodeRole.isCandidate()) {
            ControllerCandidate controllerCandidate = ControllerCandidate.getInstance();
            slotsAllocation = controllerCandidate.getSlotsAllocation();
        } else if(ServerNodeRole.isController()) {
            Controller controller = Controller.getInstance();
            slotsAllocation = controller.getSlotsAllocation();
        }

        FetchSlotsAllocationResponse.Builder builder =
                new FetchSlotsAllocationResponse.Builder();
        FetchSlotsAllocationResponse fetchSlotsAllocationResponse = builder
                .requestId(requestId)
                .slotsAllocation(slotsAllocation)
                .build();

        return fetchSlotsAllocationResponse;
    }

    /**
     * 抓取server地址列表
     * @param request
     */
    private FetchServerAddressesResponse fetchServerAddresses(
            FetchServerAddressesRequest request) {
        String requestId = request.getId();
        List<String> serverAddresses = null;

        if(ServerNodeRole.isCandidate()) {
            ControllerCandidate controllerCandidate = ControllerCandidate.getInstance();
            serverAddresses = controllerCandidate.getServerAddresses();
        } else if(ServerNodeRole.isController()) {
            Controller controller = Controller.getInstance();
            serverAddresses = controller.getServerAddresses();
        }

        FetchServerAddressesResponse.Builder builder =
                new FetchServerAddressesResponse.Builder();
        FetchServerAddressesResponse response = builder
                .requestId(requestId)
                .serverAddresses(serverAddresses)
                .build();

        return response;
    }

}
