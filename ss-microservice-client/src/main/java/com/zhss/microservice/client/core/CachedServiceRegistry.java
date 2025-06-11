package com.zhss.microservice.client.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端缓存的服务注册表
 */
public class CachedServiceRegistry {

    private CachedServiceRegistry() {

    }

    private static class Singleton {
        static CachedServiceRegistry instance = new CachedServiceRegistry();
    }

    public static CachedServiceRegistry getInstance() {
        return Singleton.instance;
    }

    /**
     * 缓存注册表
     */
    private Map<String, List<ServiceInstanceAddress>> serviceRegistry =
            new ConcurrentHashMap<>();

    /**
     * 判断服务名称是否缓存过
     * @param serviceName
     * @return
     */
    public boolean isCached(String serviceName) {
        return serviceRegistry.containsKey(serviceName);
    }

    /**
     * 获取服务的缓存好的示例地址列表
     * @param serviceName
     * @return
     */
    public List<ServiceInstanceAddress> get(String serviceName) {
        return serviceRegistry.get(serviceName);
    }

    /**
     * 缓存服务实例地址列表
     * @param serviceName
     * @param serviceInstanceAddresses
     */
    public void cache(String serviceName,
                      List<ServiceInstanceAddress> serviceInstanceAddresses) {
        serviceRegistry.put(serviceName, serviceInstanceAddresses);
    }

}
