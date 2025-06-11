package com.zhss.microservice.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * 服务治理平台的配置管理类
 */
public class Configuration {

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    /**
     * 节点的id
     */
    public static final String NODE_ID = "node.id";
    /**
     * 节点ip地址
     */
    public static final String NODE_IP = "node.ip";
    /**
     * 面向节点内部通信的TCP端口号
     */
    public static final String NODE_INTERN_TCP_PORT = "node.intern.tcp.port";
    /**
     * 面向客户端通信的HTTP端口号（提供RESTful接口的，对接可视化界面工作台）
     */
    public static final String NODE_CLIENT_HTTP_PORT = "node.client.http.port";
    /**
     * 面向客户端通信的TCP端口号
     */
    public static final String NODE_CLIENT_TCP_PORT = "node.client.tcp.port";
    /**
     * 是否为controller候选节点
     */
    public static final String IS_CONTROLLER_CANDIDATE = "is.controller.candidate";
    /**
     * 集群节点总数量
     */
    public static final String CLUSTER_NODE_COUNT = "cluster.node.count";
    /**
     * 数据存储目录
     */
    public static final String DATA_DIR = "data.dir";
    /**
     * Controller候选节点的机器列表
     */
    public static final String CONTROLLER_CANDIDATE_SERVERS = "controller.candidate.servers";
    /**
     * 心跳检查时间间隔
     */
    public static final String HEARTBEAT_CHECK_INTERVAL = "heartbeat.check.interval";
    /**
     * 判定服务实例出现故障的心跳超时时间
     */
    public static final String HEARTBEAT_TIMEOUT_PERIOD = "heartbeat.timeout.period";

    /**
     * 心跳检查时间间隔的默认值
     */
    public static final Integer DEFAULT_HEARTBEAT_CHECK_INTERVAL = 3;
    /**
     * 心跳超时时间的默认值
     */
    public static final Integer DEFAULT_HEARTBEAT_TIMEOUT_PERIOD = 5;

    /**
     * 单例
     */
    private Configuration() {

    }

    private static class Singleton {

        static Configuration instance = new Configuration();

    }

    public static Configuration getInstance() {
        return Singleton.instance;
    }

    /**
     * 节点的id
     */
    private Integer nodeId;
    /**
     * 节点ip地址
     */
    private String nodeIp;
    /**
     * 节点内部通信的TCP端口号
     */
    private Integer nodeInternTcpPort;
    /**
     * 跟客户端通信的HTTP端口号
     */
    private Integer nodeClientHttpPort;
    /**
     * 跟客户端通信的TCP端口号
     */
    private Integer nodeClientTcpPort;
    /**
     * 是否为controller候选节点
     */
    private Boolean isControllerCandidate = false;
    /**
     * 数据存储目录
     */
    private String dataDir;
    /**
     * 集群节点总数量
     */
    private Integer clusterNodeCount;
    /**
     * controller候选节点的机器列表
     */
    private String controllerCandidateServers;
    /**
     * 心跳检查时间间隔
     */
    private Integer heartbeatCheckInterval;
    /**
     * 心跳超时时间
     */
    private Integer heartbeatTimeoutPeriod;

    /**
     * 解析配置文件
     * @param configPath
     */
    public void parse(String configPath) throws ConfigurationException {
        try {
            Properties configProperties = loadConfigurationFile(configPath);

            String nodeId = configProperties.getProperty(NODE_ID);
            if(validateNodeId(nodeId)) {
                this.nodeId = Integer.valueOf(nodeId);
                LOGGER.debug("debug模式: node.id=" + nodeId);
            }

            String nodeIp = configProperties.getProperty(NODE_IP);
            if(validateNodeIp(nodeIp)) {
                this.nodeIp = nodeIp;
                LOGGER.debug("debug模式：node.ip=" + nodeIp);
            }

            String nodeInternTcpPort = configProperties.getProperty(NODE_INTERN_TCP_PORT);
            if(validateNodeInternTcpPort(nodeInternTcpPort)) {
                this.nodeInternTcpPort = Integer.valueOf(nodeInternTcpPort);
                LOGGER.debug("debug模式: node.intern.tcp.port=" + nodeInternTcpPort);
            }

            String nodeClientHttpPort = configProperties.getProperty(NODE_CLIENT_HTTP_PORT);
            if(validateNodeClientHttpPort(nodeClientHttpPort)) {
                this.nodeClientHttpPort = Integer.valueOf(nodeClientHttpPort);
                LOGGER.debug("debug模式: node.client.http.port=" + nodeClientHttpPort);
            }

            String nodeClientTcpPort = configProperties.getProperty(NODE_CLIENT_TCP_PORT);
            if(validateNodeClientTcpPort(nodeClientTcpPort)) {
                this.nodeClientTcpPort = Integer.valueOf(nodeClientTcpPort);
                LOGGER.debug("debug模式: node.client.tcp.port=" + nodeClientTcpPort);
            }

            String isControllerCandidate = configProperties.getProperty(IS_CONTROLLER_CANDIDATE);
            if(validateIsControllerCandidate(isControllerCandidate)) {
                this.isControllerCandidate = Boolean.valueOf(isControllerCandidate);
                LOGGER.debug("debug模式: is.controller.candidate=" + this.isControllerCandidate);
            }

            String clusterNodeCount = configProperties.getProperty(CLUSTER_NODE_COUNT);
            if(validateClusterNodeCount(clusterNodeCount)) {
                if(this.isControllerCandidate) {
                    this.clusterNodeCount = Integer.valueOf(clusterNodeCount);
                    LOGGER.debug("debug模式: cluster.node.count=" + clusterNodeCount);
                }
            }

            String dataDir = configProperties.getProperty(DATA_DIR);
            if(validateDataDir(dataDir)) {
                this.dataDir = dataDir;
                LOGGER.debug("debug模式: data.dir=" + dataDir);
            }

            String controllerCandidateServers = configProperties.getProperty(CONTROLLER_CANDIDATE_SERVERS);
            if(validateControllerCandidateServers(controllerCandidateServers)) {
                this.controllerCandidateServers = controllerCandidateServers;
                LOGGER.debug("debug模式: controller.candidate.servers=" + controllerCandidateServers);
            }
        } catch(IllegalArgumentException e) {
            throw new ConfigurationException("解析配置文件出现异常！！！", e);
        } catch (FileNotFoundException e) {
            throw new ConfigurationException("解析配置文件出现异常！！！", e);
        } catch (IOException e) {
            throw new ConfigurationException("解析配置文件出现异常！！！", e);
        }
    }

    /**
     * 获取数据存储目录
     * @return
     */
    public String getDataDir() {
        return dataDir;
    }

    /**
     * 加载配置文件
     * @param configPath 配置文件地址
     * @return 放入内存的配置
     */
    private Properties loadConfigurationFile(String configPath) throws IOException, IllegalArgumentException {
        File configFile = new File(configPath);

        if(!configFile.exists()) {
            throw new IllegalArgumentException("配置文件" + configPath + "不存在！！！");
        }

        Properties configProperties = new Properties();
        FileInputStream configFileInputStream = new FileInputStream(configFile);
        try {
            configProperties.load(configFileInputStream);
        } finally {
            configFileInputStream.close();
        }

        LOGGER.info("正在加载配置文件......");

        return configProperties;
    }

    /**
     * 校验节点id参数
     * @param nodeId
     * @return
     */
    private boolean validateNodeId(String nodeId) {
        if(StringUtils.isEmpty(nodeId)) {
            throw new IllegalArgumentException("node.id参数不能为空！！！");
        }

        final String regex = "(\\d+)";
        Boolean isMatch = Pattern.matches(regex, nodeId);
        if(!isMatch) {
            throw new IllegalArgumentException("node.id参数必须为数字！！！");
        }

        return true;
    }

    /**
     * 校验节点ip地址
     * @param nodeIp 节点ip地址
     * @return 校验是否通过
     * @throws IllegalArgumentException
     */
    private Boolean validateNodeIp(String nodeIp) throws IllegalArgumentException {
        if(StringUtils.isEmpty(nodeIp)) {
            throw new IllegalArgumentException("node.ip参数不能为空！！！");
        }

        final String regex = "(\\d+\\.\\d+\\.\\d+\\.\\d+)";
        Boolean isMatch = Pattern.matches(regex, nodeIp);
        if(!isMatch) {
            throw new IllegalArgumentException("node.ip参数必须符合ip地址的规范！！！");
        }

        return true;
    }

    /**
     * 校验节点内部通信TCP端口号
     * @return
     */
    private boolean validateNodeInternTcpPort(String nodeInternTcpPort) {
        if(StringUtils.isEmpty(nodeInternTcpPort)) {
            throw new IllegalArgumentException("node.intern.tcp.port参数不能为空！！！");
        }

        final String regex = "(\\d+)";
        Boolean isMatch = Pattern.matches(regex, nodeInternTcpPort);
        if(!isMatch) {
            throw new IllegalArgumentException("node.intern.tcp.port参数必须为数字！！！");
        }

        return true;
    }

    /**
     * 校验节跟客户端通信的HTTP端口号
     * @return
     */
    private boolean validateNodeClientHttpPort(String nodeClientHttpPort) {
        if(StringUtils.isEmpty(nodeClientHttpPort)) {
            throw new IllegalArgumentException("node.client.http.port参数不能为空！！！");
        }

        final String regex = "(\\d+)";
        Boolean isMatch = Pattern.matches(regex, nodeClientHttpPort);
        if(!isMatch) {
            throw new IllegalArgumentException("node.client.http.port参数必须为数字！！！");
        }

        return true;
    }

    /**
     * 校验跟客户端通信的TCP端口号
     * @return
     */
    private boolean validateNodeClientTcpPort(String nodeClientTcpPort) {
        if(StringUtils.isEmpty(nodeClientTcpPort)) {
            throw new IllegalArgumentException("node.client.tcp.port参数不能为空！！！");
        }

        final String regex = "(\\d+)";
        Boolean isMatch = Pattern.matches(regex, nodeClientTcpPort);
        if(!isMatch) {
            throw new IllegalArgumentException("node.client.tcp.port参数必须为数字！！！");
        }

        return true;
    }

    /**
     * 校验是否为controller候选节点的参数
     * @param isControllerCandidate
     * @return
     */
    private boolean validateIsControllerCandidate(String isControllerCandidate) {
        if(StringUtils.isEmpty(isControllerCandidate)) {
            throw new IllegalArgumentException("is.controller.candidate参数不能为空！！！");
        }
        if(isControllerCandidate.equals("true") || isControllerCandidate.equals("false")) {
            return true;
        }
        throw new IllegalArgumentException("is.controller.candidate参数的值必须为true或者false！！！");
    }

    /**
     * 校验集群节点总数量
     * @param clusterNodesCount
     * @return
     */
    private boolean validateClusterNodeCount(String clusterNodesCount) {
        if(isControllerCandidate && StringUtils.isEmpty(clusterNodesCount)) {
            throw new IllegalArgumentException("对于controller候选节点来说，cluster.node.count参数不能为空！！！");
        }

        if(isControllerCandidate) {
            final String regex = "(\\d+)";
            Boolean isMatch = Pattern.matches(regex, clusterNodesCount);
            if(!isMatch) {
                throw new IllegalArgumentException("cluster.node.count参数必须为数字！！！");
            }
        }

        return true;
    }

    /**
     * 校验数据存储目录配置项
     * @param dataDir
     * @return
     */
    private Boolean validateDataDir(String dataDir) {
        if(StringUtils.isEmpty(dataDir)) {
            throw new IllegalArgumentException("data.dir cannot be empty......");
        }
        return true;
    }

    /**
     * 校验controller候选节点机器列表
     * @param controllerCandidateServers controller候选节点机器列表
     * @return 校验是否通过
     * @throws IllegalArgumentException
     */
    private Boolean validateControllerCandidateServers(String controllerCandidateServers) throws IllegalArgumentException {
        if(StringUtils.isEmpty(controllerCandidateServers)) {
            throw new IllegalArgumentException("controller.candidate.servers参数不能为空！！！");
        }

        String[] controllerCandidateServersSplited = controllerCandidateServers.split(",");

        final String regex = "(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)";

        for(String controllerCandidateServer : controllerCandidateServersSplited) {
            Boolean isMatch = Pattern.matches(regex, controllerCandidateServer);
            if(!isMatch) {
                throw new IllegalArgumentException("controller.candidate.servers参数的格式不正确！！！");
            }
        }

        return true;
    }

    public Boolean isControllerCandidate() {
        return isControllerCandidate;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public Integer getNodeInternTcpPort() {
        return nodeInternTcpPort;
    }

    public Integer getNodeClientHttpPort() {
        return nodeClientHttpPort;
    }

    public Integer getNodeClientTcpPort() {
        return nodeClientTcpPort;
    }

    public Integer getClusterNodeCount() {
        return clusterNodeCount;
    }

    public String getControllerCandidateServers() {
        return controllerCandidateServers;
    }

    /**
     * 获取除自己以外的其他controller候选节点的地址
     * @return
     */
    public List<String> getOtherControllerCandidateServers() {
        List<String> otherControllerCandidateServers = new ArrayList<String>();

        Configuration configuration = Configuration.getInstance();

        String nodeIp = configuration.getNodeIp();
        Integer nodeInternTcpPort = configuration.getNodeInternTcpPort();

        String controllerCandidateServers = configuration.getControllerCandidateServers();
        String[] controllerCandidateServersSplited = controllerCandidateServers.split(",");

        for(String controllerCandidateServer : controllerCandidateServersSplited) {
            String[] controllerCandidateServerSplited = controllerCandidateServer.split(":");
            String controllerCandidateIp = controllerCandidateServerSplited[0];
            Integer controllerCandidateInternTcpPort = Integer.valueOf(controllerCandidateServerSplited[1]);

            if(!controllerCandidateIp.equals(nodeIp) ||
                    !controllerCandidateInternTcpPort.equals(nodeInternTcpPort)) {
                otherControllerCandidateServers.add(controllerCandidateServer);
            }
        }

        return otherControllerCandidateServers;
    }

    /**
     * 获取在配置文件的controller候选节点列表里，排在自己前面的节点列表
     * @return
     */
    public List<String> getBeforeControllerCandidateServers() {
        List<String> beforeControllerCandidateServers = new ArrayList<String>();

        Configuration configuration = Configuration.getInstance();

        String nodeIp = configuration.getNodeIp();
        Integer nodeInternTcpPort = configuration.getNodeInternTcpPort();

        String controllerCandidateServers = configuration.getControllerCandidateServers();
        String[] controllerCandidateServersSplited = controllerCandidateServers.split(",");

        for(String controllerCandidateServer : controllerCandidateServersSplited) {
            String[] controllerCandidateServerSplited = controllerCandidateServer.split(":");
            String controllerCandidateIp = controllerCandidateServerSplited[0];
            Integer controllerCandidateInternTcpPort = Integer.valueOf(controllerCandidateServerSplited[1]);

            if(!controllerCandidateIp.equals(nodeIp) ||
                    !controllerCandidateInternTcpPort.equals(nodeInternTcpPort)) {
                beforeControllerCandidateServers.add(controllerCandidateServer);
            } else if(controllerCandidateIp.equals(nodeIp) &&
                    controllerCandidateInternTcpPort.equals(nodeInternTcpPort)) {
                break;
            }
        }

        return beforeControllerCandidateServers;
    }

    /**
     * 获取心跳检查时间间隔
     * @return
     */
    public Integer getHeartbeatCheckInterval() {
        if(heartbeatCheckInterval == null) {
            return DEFAULT_HEARTBEAT_CHECK_INTERVAL;
        }
        return heartbeatCheckInterval;
    }

    /**
     * 获取心跳超时时间
     * @return
     */
    public Integer getHeartbeatTimeoutPeriod() {
        if(heartbeatTimeoutPeriod == null) {
            return DEFAULT_HEARTBEAT_TIMEOUT_PERIOD;
        }
        return heartbeatTimeoutPeriod;
    }

}
