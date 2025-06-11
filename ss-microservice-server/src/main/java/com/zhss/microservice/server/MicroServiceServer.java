package com.zhss.microservice.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.common.utils.StringUtils;
import com.zhss.microservice.server.config.Configuration;
import com.zhss.microservice.server.config.ConfigurationException;
import com.zhss.microservice.server.constant.NodeStatus;
import com.zhss.microservice.server.node.ServerNode;

/**
 * 微服务平台Server
 */
public class MicroServiceServer {

    /**
     * 日志组件
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroServiceServer.class);

    /**
     * 系统关闭检查的时间间隔
     */
    private static final long SHUTDOWN_CHECK_INTERVAL = 300L;

    /**
     * 服务治理平台server端的启动入口
     * @param args
     */
    public static void main(String[] args) {
        NodeStatus nodeStatus = NodeStatus.getInstance();
        nodeStatus.setStatus(NodeStatus.RUNNING);

        try {
            LOGGER.info("正在启动微服务平台Server......");

            String configPath = args[0];
            if(StringUtils.isEmpty(configPath)) {
                throw new ConfigurationException("配置文件地址不能为空！！！");
            }
            Configuration configuration = Configuration.getInstance();
            configuration.parse(configPath);

            ServerNode masterNode = new ServerNode();
            masterNode.start();

            LOGGER.info("微服务平台Server已经完成启动......");

            // 让系统无限循环等待系统停止
            waitForShutdown();
        } catch(ConfigurationException e) {
            LOGGER.error("解析配置文件时发生异常！！！", e);
            System.exit(2);
        } catch (Exception e) {
            LOGGER.error("系统发生未知的异常！！！", e);
            System.exit(1);
        }

        // 打印系统退出的日志
        if(NodeStatus.SHUTDOWN == nodeStatus.getStatus()) {
            LOGGER.info("系统即将正常关闭......");
        } else if(NodeStatus.FATAL == nodeStatus.getStatus()) {
            LOGGER.error("由于遇到不可修复的严重异常，系统即将崩溃！！！");
        }
    }

    /**
     * 等待节点被停止
     */
    private static void waitForShutdown() throws InterruptedException {
        while(NodeStatus.RUNNING == NodeStatus.get()) {
            Thread.sleep(SHUTDOWN_CHECK_INTERVAL);
        }
    }

}
