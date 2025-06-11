package com.zhss.microservice.server.node.network;

import com.zhss.microservice.common.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.UUID;

/**
 * 代表了跟一个客户端建立的长连接
 */
public class ClientConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientConnection.class);

    /**
     * 消息标识字节数
     */
    private static final Integer MESSAGE_FLAG_BYTES = 4;

    /**
     * 代表跟客户端之间的长连接的SocketChannel
     */
    private SocketChannel socketChannel;
    /**
     * 跟SocketChannel绑定在一起的一个东西
     */
    private SelectionKey selectionKey;
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
     * 是否读取完毕请求长度
     */
    private Boolean hasReadMessageLength = false;
    /**
     * 请求长度buffer
     */
    private ByteBuffer messageLengthBuffer =
            ByteBuffer.allocate(Request.REQUEST_LENGTH_BYTES);
    /**
     * 是否读取完毕请求数据
     */
    private Boolean hasReadMessage = false;
    /**
     * 请求数据buffer
     */
   private ByteBuffer messageBuffer = null;
    /**
     * 客户端连接ID
     */
   private String connectionId;

    public ClientConnection(SocketChannel socketChannel, SelectionKey selectionKey) {
        this.socketChannel = socketChannel;
        this.selectionKey = selectionKey;
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
     * 执行读IO操作
     */
    public Message readMessage() {
        try {
            // 读取请求长度
            if(!hasReadMessageLength) {
                socketChannel.read(messageLengthBuffer);

                if(messageLengthBuffer.hasRemaining()) {
                    return null;
                }

                hasReadMessageLength = true;
            }

            // 请求长度读取完毕，构建请求体buffer
            if(messageBuffer == null) {
                messageLengthBuffer.flip();
                Integer messageLength = messageLengthBuffer.getInt();
                messageBuffer = ByteBuffer.allocate(messageLength);
            }

            // 读取请求数据
            if(!hasReadMessage) {
                socketChannel.read(messageBuffer);

                if(messageBuffer.hasRemaining()) {
                    return null;
                }

                // 请求数据读取完毕了
                hasReadMessage = true;

                // 把请求数据进行反序列化，得到一个Request对象
                messageBuffer.flip();
                Integer requestType = messageBuffer.getInt();

                Message message = null;
                if(messageFlag.equals(Request.REQUEST_FLAG)) {
                    message = Request.deserialize(requestType, messageBuffer);
                } else if(messageFlag.equals(Response.RESPONSE_FLAG)) {
                    message = Response.deserialize(requestType, messageBuffer);
                }

                // 再对buffer数据做一个清空
                resetMessage();

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
    private void resetMessage() {
        hasReadMessageFlag = false;
        messageFlagBuffer = ByteBuffer.allocate(MESSAGE_FLAG_BYTES);
        messageFlag = null;
        hasReadMessageLength = false;
        messageLengthBuffer = ByteBuffer.allocate(Request.REQUEST_LENGTH_BYTES);
        hasReadMessage = false;
        messageBuffer = null;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public String getConnectionId() {
        return connectionId;
    }

}
