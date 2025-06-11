package com.zhss.microservice.server.slot.registry;

import com.zhss.microservice.server.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 服务注册表
 */
public class ServiceRegistry {

    boolean isReplica;

    public ServiceRegistry(boolean isReplica) {
        this.isReplica = isReplica;
        new HeartbeatCheckThread().start();
    }

    /**
     * 服务注册表数据结构
     */
    private ConcurrentHashMap<String, List<ServiceInstance>> serviceRegistryData =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ServiceInstance> serviceInstanceData =
            new ConcurrentHashMap<>();
    /**
     * 服务变动监听器
     */
    private ConcurrentHashMap<String, List<ServiceChangedListener>> serviceChangedListenerData =
            new ConcurrentHashMap<>();

    /**
     * 注册服务实例
     * @param serviceInstance
     */
    public void register(ServiceInstance serviceInstance) {
        // 在注册表里添加这个服务实例
        List<ServiceInstance> serviceInstances = serviceRegistryData.get(
                serviceInstance.getServiceName());
        if(serviceInstances == null) {
            synchronized (this) {
                if(serviceInstances == null) {
                    serviceInstances = new CopyOnWriteArrayList<>();
                    serviceRegistryData.put(serviceInstance.getServiceName(), serviceInstances);
                }
            }
        }
        serviceInstances.add(serviceInstance);

        serviceInstanceData.put(serviceInstance.getServiceInstanceId(),
                serviceInstance);

        // 调用服务变动监听器，执行回调通知逻辑
        if(!isReplica) {
            List<ServiceChangedListener> serviceChangedListeners =
                    serviceChangedListenerData.get(serviceInstance.getServiceName());

            if(serviceChangedListeners == null) {
                synchronized (this) {
                    if (serviceChangedListeners == null) {
                        serviceChangedListeners = new CopyOnWriteArrayList<>();
                        serviceChangedListenerData.put(serviceInstance.getServiceName(), serviceChangedListeners);
                    }
                }
            }

            for(ServiceChangedListener serviceChangedListener : serviceChangedListeners) {
                serviceChangedListener.onChange(serviceInstance.getServiceName(),
                        serviceRegistryData.get(serviceInstance.getServiceName()));
            }
        }
    }

    /**
     * 服务实例进行心跳
     * @param serviceName
     * @param serviceInstanceIp
     * @param serviceInstancePort
     */
    public void heartbeat(String serviceName,
                          String serviceInstanceIp,
                          Integer serviceInstancePort) {
        String serviceInstanceId = ServiceInstance.getServiceInstanceId(
                serviceName, serviceInstanceIp, serviceInstancePort
        );
        ServiceInstance serviceInstance = serviceInstanceData.get(serviceInstanceId);
        serviceInstance.setLatestHeartbeatTime(new Date().getTime());
        System.out.println("收到" + serviceInstanceId + "的心跳......");
    }

    /**
     * 服务订阅
     * @param serviceName
     * @return
     */
    public List<ServiceInstance> subscribe(String clientConnectionId, String serviceName) {
        List<ServiceChangedListener> serviceChangedListeners =
                serviceChangedListenerData.get(serviceName);

        if(serviceChangedListeners == null) {
            synchronized (this) {
                if (serviceChangedListeners == null) {
                    serviceChangedListeners = new CopyOnWriteArrayList<>();
                    serviceChangedListenerData.put(serviceName, serviceChangedListeners);
                }
            }
        }

        serviceChangedListeners.add(new ServiceChangedListener(clientConnectionId));

        return serviceRegistryData.get(serviceName);
    }

    /**
     * 心跳检查线程
     */
    class HeartbeatCheckThread extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(HeartbeatCheckThread.class);

        @Override
        public void run() {
            Configuration configuration = Configuration.getInstance();
            Integer heartbeatCheckInterval = configuration.getHeartbeatCheckInterval();
            Integer heartbeatTimeoutPeriod = configuration.getHeartbeatTimeoutPeriod();

            while(true) {
                long now = new Date().getTime();

                List<String> removeServiceInstanceIds = new ArrayList<String>();
                Set<String> changedServiceNames = new HashSet<String>();

                for(ServiceInstance serviceInstance : serviceInstanceData.values()) {
                    if(now - serviceInstance.getLatestHeartbeatTime() > heartbeatTimeoutPeriod * 1000L) {
                        List<ServiceInstance> serviceInstances =
                                serviceRegistryData.get(serviceInstance.getServiceName());
                        serviceInstances.remove(serviceInstance);
                        removeServiceInstanceIds.add(serviceInstance.getServiceInstanceId());
                        LOGGER.info("服务实例超过5s没有上报心跳，已经被摘除：" + serviceInstance);

                        changedServiceNames.add(serviceInstance.getServiceName());
                    }
                }

                for(String serviceInstanceId : removeServiceInstanceIds) {
                    serviceInstanceData.remove(serviceInstanceId);
                }

                // 调用服务变动监听器，执行回调通知逻辑
                if(!isReplica) {
                    for(String serviceName : changedServiceNames) {
                        List<ServiceChangedListener> serviceChangedListeners =
                                serviceChangedListenerData.get(serviceName);
                        for(ServiceChangedListener serviceChangedListener : serviceChangedListeners) {
                            serviceChangedListener.onChange(serviceName, serviceRegistryData.get(serviceName));
                        }
                    }
                }

                removeServiceInstanceIds.clear();
                changedServiceNames.clear();

                try {
                    Thread.sleep(heartbeatCheckInterval * 1000L);
                } catch(InterruptedException e) {
                    LOGGER.error("心跳检查线程遇到强制中断异常！！！", e);
                }
            }
        }
    }

}
