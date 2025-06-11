package com.zhss.microservice.client.core;

import com.zhss.microservice.common.entity.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 跟服务端通信的双向消息队列
 */
public class ServerMessageQueues {

    private ServerMessageQueues() {

    }

    private static class Singleton {
        static ServerMessageQueues instance = new ServerMessageQueues();
    }

    public static ServerMessageQueues getInstance() {
        return Singleton.instance;
    }

    /**
     * 消息队列
     */
    private Map<String, LinkedBlockingQueue<Message>> messageQueues =
            new ConcurrentHashMap<>();

    /**
     * 初始化消息队列
     * @param serverConnectionId
     */
    public void init(String serverConnectionId) {
        messageQueues.put(serverConnectionId, new LinkedBlockingQueue<>());
    }

    /**
     * 获取服务端连接的消息队列
     * @param serverConnectionId
     * @return
     */
    public void offer(String serverConnectionId, Message message) {
        LinkedBlockingQueue<Message> messageQueue =
                messageQueues.get(serverConnectionId);
        messageQueue.offer(message);
    }

    /**
     * 获取消息队列
     * @param serverConnectionId
     * @return
     */
    public LinkedBlockingQueue<Message> get(String serverConnectionId) {
        return messageQueues.get(serverConnectionId);
    }

}
