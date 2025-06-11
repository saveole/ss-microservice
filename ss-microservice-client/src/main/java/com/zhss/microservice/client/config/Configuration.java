package com.zhss.microservice.client.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.client.network.Server;
import com.zhss.microservice.common.utils.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 负责读取和解析客户端配置
 */
public class Configuration {

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    /**
     * server节点的机器列表
     */
    public static final String CONTROLLER_CANDIDATE_SERVERS = "controller.candidate.servers";
    /**
     * server节点的机器列表
     */
    public static final String SERVICE_NAME = "service.name";
    /**
     * 服务实例ip地址
     */
    public static final String SERVICE_INSTANCE_IP = "service.instance.ip";
    /**
     * 服务实例端口号
     */
    public static final String SERVICE_INSTANCE_PORT = "service.instance.port";
    /**
     * 发送心跳的时间间隔
     */
    public static final String HEARTBEAT_INTERVAL = "heartbeat.interval";

    /**
     * controller候选节点列表
     */
    private List<Server> controllerCandidates = new ArrayList<Server>();
    /**
     * 服务名称
     */
    private String serviceName;
    /**
     * 服务实例ip地址
     */
    private String serviceInstanceIp;
    /**
     * 服务实例端口号
     */
    private Integer serviceInstancePort;
    /**
     * 心跳间隔
     */
    private Integer heartbeatInterval;

    /**
     * 单例
     */
    private Configuration() {
        init();
    }

    private static class Singleton {

        static Configuration instance = new Configuration();

    }

    public static Configuration getInstance() {
        return Singleton.instance;
    }

    /**
     * 初始化配置文件
     */
    public void init() {
        try {
            // 获取本地文件路径
            String configPath = new File("").getCanonicalPath();
            configPath = configPath+"/ss-microservice-client/src/main/resources/ss-microservice-client-default.properties";

            // 加载配置文件
            Properties configProperties = loadConfigurationFile(configPath);

            // 解析和校验master节点机器列表的参数
            String servers = configProperties.getProperty(CONTROLLER_CANDIDATE_SERVERS);
            if(validateServers(servers)) {
                String[] serverArray = servers.split(",");

                for(String server : serverArray) {
                    String[] serverSplited = server.split(":");
                    String address = serverSplited[0];
                    int port = Integer.valueOf(serverSplited[1]);
                    this.controllerCandidates.add(new Server(address, port));
                }

                LOGGER.debug("debug模式: controller.candidate.servers=" + servers);
            }

            // 校验服务名称
            String serviceName = configProperties.getProperty(SERVICE_NAME);
            if(validateServiceName(serviceName)) {
                this.serviceName = serviceName;
                LOGGER.debug("debug模式: service.name=" + serviceName);
            }

            // 校验服务实例ip地址
            String serviceInstanceIp = configProperties.getProperty(SERVICE_INSTANCE_IP);
            if(validateServiceInstanceIp(serviceInstanceIp)) {
                this.serviceInstanceIp = serviceInstanceIp;
                LOGGER.debug("debug模式: service.instance.ip=" + serviceInstanceIp);
            }

            // 校验服务实例端口号
            String serviceInstancePort = configProperties.getProperty(SERVICE_INSTANCE_PORT);
            if(validateServiceInstancePort(serviceInstancePort)) {
                this.serviceInstancePort = Integer.valueOf(serviceInstancePort);
                LOGGER.debug("debug模式: service.instance.port=" + serviceInstancePort);
            }

            // 校验心跳间隔
            String heartbeatInterval = configProperties.getProperty(HEARTBEAT_INTERVAL);
            if(validateHeartbeatInterval(heartbeatInterval)) {
                this.heartbeatInterval = Integer.valueOf(heartbeatInterval);
                LOGGER.debug("debug模式：heartbeat.interval=" + heartbeatInterval);
            }
        } catch(IllegalArgumentException e) {
            LOGGER.error("parsing config file error", e);
        } catch (FileNotFoundException e) {
            LOGGER.error("parsing config file error", e);
        } catch (IOException e) {
            LOGGER.error("parsing config file error", e);
        }
    }



    /**
     * 加载配置文件
     * @param configPath 配置文件地址
     * @return 放入内存的配置
     */
    private Properties loadConfigurationFile(String configPath) throws IOException, IllegalArgumentException {
        File configFile = new File(configPath);

        if(!configFile.exists()) {
            throw new IllegalArgumentException("config file " + configPath + " doesn't exist......");
        }

        Properties configProperties = new Properties();
        FileInputStream configFileInputStream = new FileInputStream(configFile);
        try {
            configProperties.load(configFileInputStream);
        } finally {
            configFileInputStream.close();
        }

        return configProperties;
    }

    /**
     * 校验server节点机器列表参数
     */
    private Boolean validateServers(String servers) throws IllegalArgumentException {
        String[] serverArray = servers.split(",");
        if(serverArray == null || serverArray.length == 0) {
            throw new IllegalArgumentException("ss.govern.servers cannot be empty.....");
        }

        final String regex = "(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)";

        for(String server : serverArray) {
            Boolean isMatch = Pattern.matches(regex, server);
            if(!isMatch) {
                throw new IllegalArgumentException("ss.govern.servers parameter has a wrong pattern: " + servers);
            }
        }

        return true;
    }

    /**
     * 校验服务实例ip地址
     */
    private Boolean validateServiceInstanceIp(String serviceInstanceIp) throws IllegalArgumentException {
        final String regex = "(\\d+\\.\\d+\\.\\d+\\.\\d+)";

        Boolean isMatch = Pattern.matches(regex, serviceInstanceIp);
        if(!isMatch) {
            throw new IllegalArgumentException("service.instance.ip parameter has a wrong pattern: " + serviceInstanceIp);
        }

        return true;
    }

    /**
     * 校验服务实例端口号
     */
    private Boolean validateServiceInstancePort(String serviceInstancePort) throws IllegalArgumentException {
        final String regex = "(\\d+)";

        Boolean isMatch = Pattern.matches(regex, serviceInstancePort);
        if(!isMatch) {
            throw new IllegalArgumentException("service.instance.port must be a number: " + serviceInstancePort);
        }

        return true;
    }

    /**
     * 校验心跳间隔时间
     */
    private Boolean validateHeartbeatInterval(String heartbeatInterval) throws IllegalArgumentException {
        final String regex = "(\\d+)";

        Boolean isMatch = Pattern.matches(regex, heartbeatInterval);
        if(!isMatch) {
            throw new IllegalArgumentException("heartbeat.interval must be a number: " + heartbeatInterval);
        }

        return true;
    }

    /**
     * 校验服务名称
     */
    private Boolean validateServiceName(String serviceName) throws IllegalArgumentException {
        if(StringUtils.isEmpty(serviceName)) {
            throw new IllegalArgumentException("service.name cannot be empty.....");
        }
        return true;
    }

    /**
     * 获取servers地址列表
     * @return
     */
    public List<Server> getControllerCandidates() {
        return controllerCandidates;
    }

    /**
     * 获取服务名称
     * @return
     */
    public String getServiceName() {
        return serviceName;
    }

    public String getServiceInstanceIp() {
        return serviceInstanceIp;
    }

    public Integer getServiceInstancePort() {
        return serviceInstancePort;
    }

    public Integer getHeartbeatInterval() {
        return heartbeatInterval;
    }

}
