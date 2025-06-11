package com.zhss.microservice.server.node.network;

import com.zhss.microservice.common.entity.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.common.entity.Request;
import com.zhss.microservice.common.entity.Response;
import com.zhss.microservice.server.config.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * master节点的NIO服务器
 * 他主要是用于跟客户端建立连接，进行网络通信的
 */
public class ClientNIOServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientNIOServer.class);

    private ClientNIOServer() {
        try {
            this.selector  = Selector.open();
        } catch(IOException e) {
            LOGGER.error("selector open error......", e);
        }
    }

    static class Singleton {
        static ClientNIOServer instance = new ClientNIOServer();
    }

    public static ClientNIOServer getInstance() {
        return Singleton.instance;
    }

    /**
     * NIO的多路复用组件
     */
    private Selector selector;
    /**
     * NIO的Server端网络通信组件
     */
    private ServerSocketChannel serverSocketChannel;

    /**
     * 启动NIO服务器
     */
    public void start() {
        try {
            // 启动NIO服务器组件，监听指定的端口号
            Configuration configuration = Configuration.getInstance();
            int clientNetworkPort = configuration.getNodeClientTcpPort();
            InetSocketAddress socketAddress = new InetSocketAddress(clientNetworkPort);

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().setReuseAddress(true);
            serverSocketChannel.socket().bind(socketAddress);
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            LOGGER.info("NIO server binding to port[" + clientNetworkPort + "]......");

            // 启动NIO服务器的网络IO线程
            NetworkIOThread networkIOThread = new NetworkIOThread();
            networkIOThread.setDaemon(true);
            networkIOThread.start();
        } catch (IOException e) {
            LOGGER.error("start NIO server error.......", e);
        }
    }

    /**
     * 网络IO线程
     *
     * 实现一个线程，到底是用Runnable接口，还是Thread类
     * 如果说你要封装的是线程的执行逻辑，那么就继承自Runnable接口就可以了
     * 后续你需要把Runnable接口的实现类对象塞到一个Thread对象里去，作为一个线程的执行逻辑
     * 但是如果你封装的直接就代表了一个线程，那么就用Thread类就可以了
     *
     */
    class NetworkIOThread extends Thread {

        public void run() {
            // 只要ServerSocketChannel还没有关闭
            // 此时就无限循环去处理各种各样的连接和请求
            while(!serverSocketChannel.socket().isClosed()) {
                try {
                    selector.select(1000);

                    Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    if(selectionKeys == null || selectionKeys.size() == 0) {
                        continue;
                    }

                    Iterator<SelectionKey> selectionKeyIterator = selectionKeys.iterator();

                    while(selectionKeyIterator.hasNext()) {
                        SelectionKey selectionKey = selectionKeyIterator.next();

                        // 这里的SelectionKey代表的是ServerSocketChannel
                        if ((selectionKey.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                            // 跟客户端完成三次握手建立TCP长连接
                            ServerSocketChannel serverSocketChannel =
                                    (ServerSocketChannel)selectionKey.channel();
                            SocketChannel socketChannel = serverSocketChannel.accept();

                            socketChannel.configureBlocking(false);

                            // 把跟客户端建立好的连接SocketChannel注册到Selector里去
                            SelectionKey clientSelectionKey = socketChannel.register(
                                    selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

                            // 维护好客户端连接数据
                            ClientConnection clientConnection = new ClientConnection(
                                    socketChannel, clientSelectionKey);

                            ClientConnectionManager clientConnectionManager = ClientConnectionManager.getInstance();
                            clientConnectionManager.addClientConnection(clientConnection);

                            ClientMessageQueues clientMessageQueues = ClientMessageQueues.getInstance();
                            clientMessageQueues.initMessageQueue(clientConnection.getConnectionId());

                            clientSelectionKey.attach(clientConnection);

                            LOGGER.info("跟客户端建立连接: " +
                                    socketChannel.socket().getRemoteSocketAddress());

                            selectionKeyIterator.remove();
                        }
                        // 处理客户端连接的请求/响应
                        else if ((selectionKey.readyOps() & SelectionKey.OP_READ) != 0) {
                            if(selectionKey.isReadable()) {
                                ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();

                                Integer messageFlag = clientConnection.readMessageFlag();

                                if(messageFlag != null) {
                                    if(messageFlag.equals(Request.REQUEST_FLAG)) {
                                        Message message = clientConnection.readMessage();

                                        if (message != null) {
                                            if(message instanceof Request) {
                                                Request request = (Request) message;
                                                ClientRequestProcessor clientRequestProcessor = ClientRequestProcessor.getInstance();
                                                Response response = clientRequestProcessor.process(
                                                        clientConnection.getConnectionId(), request);
                                                ClientMessageQueues clientMessageQueues = ClientMessageQueues.getInstance();
                                                clientMessageQueues.offerMessage(clientConnection.getConnectionId(), response);
                                            } else if(message instanceof  Response) {
                                                Response response = (Response) message;
                                                System.out.println("服务端推送的请求收到响应，requestId=" + response.getRequestId());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        // 返回响应给客户端 / 发送请求给客户端
                        else if((selectionKey.readyOps() & SelectionKey.OP_WRITE) != 0) {
                            if(selectionKey.isWritable()) {
                                ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();
                                if(clientConnection == null) {
                                    continue;
                                }

                                // 处理需要返回给客户端的响应
                                ClientMessageQueues clientMessageQueues = ClientMessageQueues.getInstance();
                                LinkedBlockingQueue<Message> messageQueue = clientMessageQueues.getMessageQueue(
                                        clientConnection.getConnectionId());
                                if(messageQueue.isEmpty()) {
                                    continue;
                                }

                                Message message = messageQueue.peek();
                                if(message == null) {
                                    continue;
                                }

                                ByteBuffer data = message.getData();

                                SocketChannel socketChannel = clientConnection.getSocketChannel();
                                socketChannel.write(data);

                                if(!data.hasRemaining()) {
                                    messageQueue.poll();
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    LOGGER.error("network IO error......", e);
                }
            }
        }

    }

}
