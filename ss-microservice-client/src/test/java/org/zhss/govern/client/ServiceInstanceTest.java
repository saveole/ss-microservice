package org.zhss.govern.client;

import com.zhss.microservice.client.core.ServiceInstance;

public class ServiceInstanceTest {

    public static void main(String[] args) throws Exception {
        ServiceInstance serviceInstanceClient = new ServiceInstance();
        serviceInstanceClient.init();
        serviceInstanceClient.register();
        serviceInstanceClient.startHeartbeatScheduler();
        serviceInstanceClient.subscribe("ORDER-SERVICE");
    }

}
