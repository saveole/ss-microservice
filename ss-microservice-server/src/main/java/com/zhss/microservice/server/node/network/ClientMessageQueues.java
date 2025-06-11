package com.zhss.microservice.server.node.network;

import com.zhss.microservice.common.entity.Message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 针对客户端的双向消息队列：请求 or 响应
 */
public class ClientMessageQueues {

    private ClientMessageQueues() {

    }

    private static class Singleton {

        static ClientMessageQueues instance = new ClientMessageQueues();

    }

    public static ClientMessageQueues getInstance() {
        return Singleton.instance;
    }

    private ConcurrentHashMap<String, LinkedBlockingQueue<Message>> messageQueues =
            new ConcurrentHashMap<>();

    public void initMessageQueue(String clientConnectionId) {
        messageQueues.put(clientConnectionId, new LinkedBlockingQueue<>());
    }

    public void offerMessage(String clientConnectionId, Message message) {
        LinkedBlockingQueue<Message> messageQueue = messageQueues.get(clientConnectionId);
        messageQueue.offer(message);
    }

    public LinkedBlockingQueue<Message> getMessageQueue(String clientConnectionId) {
        return messageQueues.get(clientConnectionId);
    }

}
