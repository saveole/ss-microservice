package com.zhss.microservice.client.network;

import com.zhss.microservice.common.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.UUID;

/**
 * 客户端跟一个服务端建立的长连接
 */
public class ServerConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnection.class);

    /**
     * 消息标识字节数
     */
    private static final Integer MESSAGE_FLAG_BYTES = 4;
    /**
     * 消息长度字节数
     */
    private static final Integer MESSAGE_LENGTH_BYTES = 4;

    /**
     * 代表客户端和server端连接的SelectionKey
     */
    private SelectionKey selectionKey;
    /**
     * 客户端和服务端的连接
     */
    private SocketChannel socketChannel;
    /**
     * 服务端连接id
     */
    private String connectionId;
    /**
     * 是否读取完毕消息标识（请求/响应）
     */
    private Boolean hasReadMessageFlag = false;
    /**
     * 消息标识buffer
     */
    private ByteBuffer messageFlagBuffer =
            ByteBuffer.allocate(MESSAGE_FLAG_BYTES);
    /**
     * 消息标识
     */
    private Integer messageFlag = null;
    /**
     * 是否读取完毕响应长度
     */
    private Boolean hasReadMessageLength = false;
    /**
     * 响应长度buffer
     */
    private ByteBuffer messageLengthBuffer =
            ByteBuffer.allocate(MESSAGE_LENGTH_BYTES);
    /**
     * 是否读取完毕响应数据
     */
    private Boolean hasReadMessage = false;
    /**
     * 响应数据buffer
     */
    private ByteBuffer messageBuffer = null;

    public ServerConnection(
            SelectionKey selectionKey,
            SocketChannel socketChannel) {
        this.selectionKey = selectionKey;
        this.socketChannel = socketChannel;
        this.connectionId = UUID.randomUUID().toString().replace(
                "-", "");
    }

    /**
     * 读取消息标识
     * @return
     */
    public Integer readMessageFlag() {
        try {
            if(!hasReadMessageFlag) {
                socketChannel.read(messageFlagBuffer);

                if(messageFlagBuffer.hasRemaining()) {
                    return null;
                }

                hasReadMessageFlag = true;

                messageFlagBuffer.flip();
                messageFlag = messageFlagBuffer.getInt();
                return messageFlag;
            } else {
                return messageFlag;
            }
        } catch(Exception e) {
            LOGGER.error("read flag error......", e);
        }

        return null;
    }

    /**
     * 读取消息
     */
    public Message readMessage() {
        try {
            if(!hasReadMessageLength) {
                socketChannel.read(messageLengthBuffer);

                if(messageLengthBuffer.hasRemaining()) {
                    return null;
                }

                hasReadMessageLength = true;
            }

            if(messageBuffer == null) {
                messageLengthBuffer.flip();
                Integer messageLength = messageLengthBuffer.getInt();
                messageBuffer = ByteBuffer.allocate(messageLength);
            }

            if(!hasReadMessage) {
                socketChannel.read(messageBuffer);

                if(messageBuffer.hasRemaining()) {
                    return null;
                }

                hasReadMessage = true;

                messageBuffer.flip();
                Integer requestType = messageBuffer.getInt();

                Message message = null;
                if(messageFlag.equals(Response.RESPONSE_FLAG)) {
                    message = Response.deserialize(requestType, messageBuffer);
                } else if(messageFlag.equals(Request.REQUEST_FLAG)) {
                    message = Request.deserialize(requestType, messageBuffer);
                }

                // 再对buffer数据做一个清空
                reset();

                // 返回解析到的请求对象
                return message;
            }
        } catch(IOException e) {
            LOGGER.error("do read io error......", e);
        }
        return null;
    }

    /**
     * 重置读取的请求数据
     */
    private void reset() {
        hasReadMessageFlag = false;
        messageFlagBuffer = ByteBuffer.allocate(MESSAGE_FLAG_BYTES);
        messageFlag = null;
        hasReadMessageLength = false;
        messageLengthBuffer = ByteBuffer.allocate(MESSAGE_LENGTH_BYTES);
        hasReadMessage = false;
        messageBuffer = null;
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public String getConnectionId() {
        return connectionId;
    }

}
