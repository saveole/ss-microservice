package com.zhss.microservice.server.replica;

import com.zhss.microservice.common.entity.HeartbeatRequest;
import com.zhss.microservice.common.entity.RegisterRequest;
import com.zhss.microservice.common.entity.Request;
import com.zhss.microservice.server.constant.NodeStatus;
import com.zhss.microservice.server.node.network.ServerMessageReceiver;
import com.zhss.microservice.server.slot.Slot;
import com.zhss.microservice.server.slot.SlotManager;
import com.zhss.microservice.server.slot.registry.ServiceInstance;
import com.zhss.microservice.server.slot.registry.ServiceRegistry;

/**
 * 副本复制的组件
 */
public class ReplicationManager {

    private ReplicationManager() {

    }

    static class Singleton {
        static ReplicationManager instance = new ReplicationManager();
    }

    public static ReplicationManager getInstance() {
        return Singleton.instance;
    }

    public void start() {
        new ReplicationThread().start();
    }

    class ReplicationThread extends Thread {

        public void run() {
            ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
            SlotManager slotManager = SlotManager.getInstance();

            while(NodeStatus.isRunning()) {
                Request replicaRequest = serverMessageReceiver.takeReplicaRequest();

                if(replicaRequest instanceof RegisterRequest) {
                    RegisterRequest registerRequest = (RegisterRequest) replicaRequest;

                    String serviceName = registerRequest.getServiceName();
                    String serviceInstanceIp = registerRequest.getServiceInstanceIp();
                    Integer serviceInstancePort = registerRequest.getServiceInstancePort();

                    ServiceInstance serviceInstance = new ServiceInstance(
                            serviceName,
                            serviceInstanceIp,
                            serviceInstancePort
                    );

                    Slot slotReplica = slotManager.getSlotReplica(serviceName);
                    ServiceRegistry serviceRegistry = slotReplica.getServiceRegistry();
                    serviceRegistry.register(serviceInstance);
                } else if(replicaRequest instanceof HeartbeatRequest) {
                    HeartbeatRequest heartbeatRequest = (HeartbeatRequest) replicaRequest;

                    String serviceName = heartbeatRequest.getServiceName();
                    String serviceInstanceIp = heartbeatRequest.getServiceInstanceIp();
                    Integer serviceInstancePort = heartbeatRequest.getServiceInstancePort();

                    Slot slotReplica = slotManager.getSlotReplica(serviceName);
                    ServiceRegistry serviceRegistry = slotReplica.getServiceRegistry();
                    serviceRegistry.heartbeat(serviceName, serviceInstanceIp, serviceInstancePort);
                }
            }
        }

    }

}
