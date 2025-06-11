package com.zhss.microservice.client.network;

import com.zhss.microservice.client.core.CachedServiceRegistry;
import com.zhss.microservice.client.core.ServiceInstanceAddress;
import com.zhss.microservice.common.entity.Request;
import com.zhss.microservice.common.entity.Response;
import com.zhss.microservice.common.entity.ServiceChangedRequest;
import com.zhss.microservice.common.entity.ServiceChangedResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 服务端请求处理组件
 */
public class ServerRequestProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerRequestProcessor.class);

    private ServerRequestProcessor() {

    }

    private static class Singleton {
        static ServerRequestProcessor instance = new ServerRequestProcessor();
    }

    public static ServerRequestProcessor getInstance() {
        return Singleton.instance;
    }

    public Response process(Request request) {
        if(request instanceof ServiceChangedRequest) {
             return serviceChanged((ServiceChangedRequest) request);
        }
        return null;
    }

    /**
     * 处理服务变更的事件
     * @param request
     * @return
     */
    private Response serviceChanged(ServiceChangedRequest request) {
        // 处理服务实例地址数据
        String serviceName = request.getServiceName();
        List<String> rawServiceInstanceAddresses = request.getServiceInstanceAddresses();

        List<ServiceInstanceAddress> serviceInstanceAddresses =
                new ArrayList<ServiceInstanceAddress>();

        for(String rawServiceInstanceAddress : rawServiceInstanceAddresses) {
            String[] rawServiceInstanceAddressSplited = rawServiceInstanceAddress.split(",");
            ServiceInstanceAddress serviceInstanceAddress = new ServiceInstanceAddress(
                    rawServiceInstanceAddressSplited[0],
                    rawServiceInstanceAddressSplited[1],
                    Integer.valueOf(rawServiceInstanceAddressSplited[2])
            );
            serviceInstanceAddresses.add(serviceInstanceAddress);
        }

        // 刷新服务实例地址的缓存
        CachedServiceRegistry cachedServiceRegistry = CachedServiceRegistry.getInstance();
        cachedServiceRegistry.cache(serviceName, serviceInstanceAddresses);
        LOGGER.info("服务器推送服务【" + serviceName + "】最新地址列表：" + serviceInstanceAddresses);

        // 构建响应对象
        ServiceChangedResponse response = new ServiceChangedResponse.Builder()
                .requestId(request.getId())
                .result(ServiceChangedResponse.SUCCESS)
                .build();

        return response;
    }

}
