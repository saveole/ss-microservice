package com.zhss.microservice.server.node.network;

import com.zhss.microservice.server.constant.MessageType;
import com.zhss.microservice.server.constant.NodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.server.config.Configuration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 集群内部的master节点之间进行网络通信的管理组件
 *
 * 1、跟其他的master节点建立网络连接，避免出现重复的连接
 * 2、在底层基于队列和线程，帮助我们发送请求给其他的机器节点
 * 3、同上，需要接受其他节点发送过来的请求，交给我们来进行业务逻辑上的处理
 *
 */
public class ServerNetworkManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerNetworkManager.class);

    /**
     * 默认的主动连接的重试次数
     */
    private static final int DEFAULT_CONNECT_RETRIES = 3;
    /**
     * 连接超时时间
     */
    private static final int CONNECT_TIMEOUT = 5000;
    /**
     * 重试连接master node的时间间隔
     */
    private static final long RETRY_CONNECT_MASTER_NODE_INTERVAL = 1 * 60 * 1000;
    /**
     * 检查跟其他所有节点的连接状态的时间间隔
     */
    private static final long CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL = 10 * 1000;
    /**
     * 等待所有master节点连接过来的检查间隔
     */
    private static final Long ALL_MASTER_NODE_CONNECT_CHECK_INTERVAL = 100L;

    private ServerNetworkManager() {
        new RetryConnectMasterNodeThread().start();
    }

    static class Singleton {
        static ServerNetworkManager instance = new ServerNetworkManager();
    }

    public static ServerNetworkManager getInstance() {
        return Singleton.instance;
    }

    /**
     * 等待重试发起连接的master节点列表
     */
    private CopyOnWriteArrayList<String> retryConnectMasterNodes =
            new CopyOnWriteArrayList<String>();
    /**
     * 跟其他的远程master节点建立好的连接
     */
    private ConcurrentHashMap<Integer, Socket> remoteNodeSockets =
            new ConcurrentHashMap<Integer, Socket>();
    /**
     * 每个节点连接的读写IO线程是否运行的boolean变量
     */
    private ConcurrentHashMap<Integer, IOThreadRunningSignal> ioThreadRunningSignals =
            new ConcurrentHashMap<>();
    /**
     * 发送请求队列
     */
    private ConcurrentHashMap<Integer, LinkedBlockingQueue<ByteBuffer>> sendQueues =
            new ConcurrentHashMap<Integer, LinkedBlockingQueue<ByteBuffer>>();
    /**
     * 接收请求队列
     */
    private LinkedBlockingQueue<ByteBuffer> receiveQueue =
            new LinkedBlockingQueue<ByteBuffer>();

    /**
     * 启动server连接请求的监听器
     */
    public void startServerConnectionListener() {
        new ServerConnectionListener().start();
    }

    /**
     * 连接所有的Controller候选节点
     * @return
     */
    public Boolean connectAllControllerCandidates() {
        Configuration configuration = Configuration.getInstance();
        String controllerCandidateServers = configuration.getControllerCandidateServers();
        String[] controllerCandidateServersSplited = controllerCandidateServers.split(",");

        for(String controllerCandidateServer : controllerCandidateServersSplited) {
            if(!connectServerNode(controllerCandidateServer)) {
                continue;
            }
        }

        return true;
    }

    /**
     * 连接在配置文件里排在自己前面的controller候选节点
     * @return
     */
    public Boolean connectBeforeControllerCandidateServers() {
        Configuration configuration = Configuration.getInstance();

        List<String> beforeControllerCandidateServers =
                configuration.getBeforeControllerCandidateServers();
        if(beforeControllerCandidateServers.size() == 0) {
            return true;
        }

        for(String beforeControllerCandidateServer : beforeControllerCandidateServers) {
            if(!connectServerNode(beforeControllerCandidateServer)) {
                continue;
            }
        }

        return true;
    }

    /**
     * 连接server节点
     * @param serverNode
     * @return
     */
    private Boolean connectServerNode(String serverNode) {
        boolean fatal = false;

        String[] serverNodeSplited = serverNode.split(":");
        String ip = serverNodeSplited[0];
        int port = Integer.valueOf(serverNodeSplited[1]);

        InetSocketAddress endpoint = new InetSocketAddress(ip, port);

        int retries = 0;

        while(NodeStatus.RUNNING == NodeStatus.get() &&
                retries <= DEFAULT_CONNECT_RETRIES) {
            try {
                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(0);
                socket.connect(endpoint, CONNECT_TIMEOUT);

                if(!sendSelfInformation(socket)) {
                    fatal = true;
                    break;
                }
                RemoteServerNode remoteServerNode = readRemoteNodeInformation(socket);
                if(remoteServerNode == null) {
                    fatal = true;
                    break;
                }

                startServerIOThreads(remoteServerNode.getNodeId(), socket);
                addRemoteNodeSocket(remoteServerNode.getNodeId(), socket);
                RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
                remoteServerNodeManager.addRemoteServerNode(remoteServerNode);

                LOGGER.info("完成与远程server节点的连接："+ remoteServerNode + "......");

                return true;
            } catch(IOException e) {
                LOGGER.error("与server节点(" + endpoint + ") 建立连接的过程中发生异常！！！");

                retries++;
                if(retries <= DEFAULT_CONNECT_RETRIES) {
                    LOGGER.error("这是第" + retries + "次重试连接server节点(" + endpoint + ")......");
                }
            }
        }

        // 出现不可逆转的异常，系统崩溃
        if(fatal) {
            NodeStatus nodeStatus = NodeStatus.getInstance();
            nodeStatus.setStatus(NodeStatus.FATAL);
            return false;
        }

        // 连续几次重试都不行，此时节点加入待重试列表
        if(!retryConnectMasterNodes.contains(serverNode)) {
            retryConnectMasterNodes.add(serverNode);
            LOGGER.error("连接server节点(" + serverNode + ") 失败, 将该节点加入重试列表中......");
        }

        return false;
    }

    /**
     * 发送自己的信息对对方节点
     * @param socket
     */
    public boolean sendSelfInformation(Socket socket) {
        Configuration configuration = Configuration.getInstance();

        Integer nodeId = configuration.getNodeId();
        Boolean isControllerCandidate = configuration.isControllerCandidate();
        String ip = configuration.getNodeIp();
        Integer clientTcpPort = configuration.getNodeClientTcpPort();

        DataOutputStream outputStream = null;
        try {
            outputStream = new DataOutputStream(socket.getOutputStream());
            outputStream.writeInt(nodeId);
            outputStream.writeInt(isControllerCandidate ? 1 : 0);
            outputStream.writeInt(ip.length());
            outputStream.write(ip.getBytes());
            outputStream.writeInt(clientTcpPort);
            outputStream.flush();
        } catch (IOException e) {
            LOGGER.error("发送本节点信息给刚建立连接的server节点，出现通信异常！！！", e);

            try {
                socket.close();
            } catch(IOException ex) {
                LOGGER.error("关闭Socket连接异常！！！", ex);
            }

            return false;
        }

        return true;
    }

    /**
     * 读取其他节点发送过来的信息
     * @param socket
     * @return
     */
    public RemoteServerNode readRemoteNodeInformation(Socket socket) {
        try {
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());

            Integer remoteNodeId = inputStream.readInt();
            Integer isControllerCandidateValue = inputStream.readInt();
            Boolean isControllerCandidate = isControllerCandidateValue == 1 ? true : false;

            Integer ipLength = inputStream.readInt();
            byte[] ipBytes = new byte[ipLength];
            inputStream.read(ipBytes);
            String ip = new String(ipBytes);

            Integer clientPort = inputStream.readInt();

            RemoteServerNode remoteServerNode = new RemoteServerNode(
                    remoteNodeId,
                    isControllerCandidate,
                    ip,
                    clientPort
            );

            return remoteServerNode;
        } catch (IOException e) {
            LOGGER.error("从刚刚连接连接的server节点读取信息发生异常！！！", e);

            try {
                socket.close();
            } catch(IOException ex) {
                LOGGER.error("Socket关闭异常！！！", ex);
            }
        }
        return null;
    }

    /**
     * 为建立成功的连接启动IO线程
     * @param socket
     */
    public void startServerIOThreads(Integer remoteNodeId, Socket socket) {
        // 初始化发送请求队列
        LinkedBlockingQueue<ByteBuffer> sendQueue =
                new LinkedBlockingQueue<ByteBuffer>();
        sendQueues.put(remoteNodeId, sendQueue);

        // 除了初始化IO线程，还应该初始化IO线程使用的队列
        IOThreadRunningSignal ioThreadRunningSignal = new IOThreadRunningSignal(true);
        ioThreadRunningSignals.put(remoteNodeId, ioThreadRunningSignal);

        new ServerWriteIOThread(remoteNodeId, socket, sendQueue, ioThreadRunningSignal).start();
        new ServerReadIOThread(remoteNodeId, socket, receiveQueue, this, ioThreadRunningSignal).start();
    }

    /**
     * 添加跟远程节点建立好的连接
     * @param remoteNodeId
     * @param socket
     */
    public void addRemoteNodeSocket(Integer remoteNodeId, Socket socket) {
        this.remoteNodeSockets.put(remoteNodeId, socket);
    }

    /**
     * 添加一个建立好连接的远程master节点
     * @param remoteServerNode
     */
    public void addRemoteServerNode(RemoteServerNode remoteServerNode) {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        remoteServerNodeManager.addRemoteServerNode(remoteServerNode);
    }

    /**
     * 等待跟所有的master节点建立连接
     */
    public void waitAllServerNodeConnected() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();

        Configuration configuration = Configuration.getInstance();
        Integer clusterNodeCount = configuration.getClusterNodeCount();

        Boolean allServerNodeConnected = false;

        LOGGER.info("等待跟所有的server节点都建立连接......");

        while(!allServerNodeConnected) {
            try {
                Thread.sleep(ALL_MASTER_NODE_CONNECT_CHECK_INTERVAL);
            } catch (InterruptedException e) {
                LOGGER.error("线程中断异常！！！", e);
            }
            if(clusterNodeCount == remoteServerNodeManager.getRemoteServerNodes().size() + 1) {
                allServerNodeConnected = true;
            }
        }

        LOGGER.info("已经跟所有的server节点都建立连接......");
    }

    /**
     * 等待跟所有的controller候选节点完成连接
     */
    public void waitAllControllerCandidatesConnected() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();

        Configuration configuration = Configuration.getInstance();
        List<String> otherControllerCandidateServers = configuration.getOtherControllerCandidateServers();

        LOGGER.info("正在等待跟所有Controller候选节点建立连接: " + otherControllerCandidateServers + "......");

        while(NodeStatus.RUNNING == NodeStatus.get()) {
            boolean allControllerCandidatesConnected = false;

            List<RemoteServerNode> connectedControllerCandidates =
                    remoteServerNodeManager.getOtherControllerCandidates();
            if(connectedControllerCandidates.size() == otherControllerCandidateServers.size()) {
                allControllerCandidatesConnected = true;
            }

            if(allControllerCandidatesConnected) {
                LOGGER.info("已经跟所有Controller候选节点建立连接......");
                break;
            }

            try {
                Thread.sleep(CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL);
            } catch(InterruptedException e) {
                LOGGER.error("发生线程中断异常！！！", e);
            }
        }
    }

    /**
     * 发送网络请求给远程的其他节点
     * @param remoteNodeId
     * @param message
     * @return
     */
    public Boolean sendMessage(Integer remoteNodeId, ByteBuffer message) {
        try {
            LinkedBlockingQueue<ByteBuffer> sendQueue = sendQueues.get(remoteNodeId);
            if(sendQueue != null) {
                sendQueue.put(message);
            }
        } catch(InterruptedException e) {
            LOGGER.error("put message into send queue error, remoteNodeId=" + remoteNodeId, e);
            return false;
        }
        return true;
    }

    /**
     * 阻塞在这里获取消息
     * @return
     */
    public ByteBuffer takeMessage() {
        try {
            return receiveQueue.take();
        } catch (Exception e) {
            LOGGER.error("take message from receive queue error......", e);
        }
        return null;
    }

    /**
     * 重试连接master node的线程
     */
    class RetryConnectMasterNodeThread extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(RetryConnectMasterNodeThread.class);

        @Override
        public void run() {
            while(NodeStatus.RUNNING == NodeStatus.get()) {
                // 每隔5分钟运行一次定时重试机制
                List<String> retryConnectSuccessMasterNodes = new ArrayList<String>();

                for(String retryConnectMasterNode : retryConnectMasterNodes ) {
                    LOGGER.error("scheduled retry connect master node: "+ retryConnectMasterNode + ".......");
                    if(connectServerNode(retryConnectMasterNode)) {
                        retryConnectSuccessMasterNodes.add(retryConnectMasterNode);
                    }
                }

                // 只要重试成功了，就可以把这个节点从定时重试列表里移除就可以了
                for(String retryConnectSuccessMasterNode : retryConnectSuccessMasterNodes) {
                    retryConnectMasterNodes.remove(retryConnectSuccessMasterNode);
                }

                try {
                    Thread.sleep(RETRY_CONNECT_MASTER_NODE_INTERVAL);
                } catch (InterruptedException e) {
                    LOGGER.error("RetryConnectMasterNodeThread is interrupted because of unknown reasons......");
                }
            }
        }
    }

    /**
     * 网络连接监听线程
     */
    class ServerConnectionListener extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(ServerConnectionListener.class);

        /**
         * 默认的监听端口号的重试次数
         */
        public static final int DEFAULT_RETRIES = 3;

        /**
         * 网络连接监听服务器
         */
        private ServerSocket serverSocket;
        /**
         * 当前已经尝试重试监听端口号的次数
         */
        private int retries = 0;

        /**
         * 线程的运行逻辑
         */
        @Override
        public void run() {
            Configuration configuration = Configuration.getInstance();

            // 在线程运行期间是否遇到了出乎意料之外的崩溃异常
            boolean fatal = false;

            // 只要系统还在运行，而且监听端口号的重试次数小于默认重试次数
            while(NodeStatus.RUNNING == NodeStatus.get()
                    && retries <= DEFAULT_RETRIES) {
                try {
                    // 获取master节点内部网络通信的端口号
                    int port = configuration.getNodeInternTcpPort();
                    InetSocketAddress endpoint = new InetSocketAddress(port);

                    // 基于ServerSocket监听master节点内部网络通信的端口号
                    this.serverSocket = new ServerSocket();
                    this.serverSocket.setReuseAddress(true);
                    this.serverSocket.bind(endpoint);

                    LOGGER.info("server连接请求线程，已经绑定端口号: " + port + "，等待监听连接请求......");

                    // 跟发起连接请求的master建立网络连接
                    while(NodeStatus.RUNNING == NodeStatus.get()) {
                        // id比自己大的master节点发送网络连接请求过来
                        // 在这里会成功建立网路连接
                        Socket socket = this.serverSocket.accept();
                        socket.setTcpNoDelay(true); // 网络通信不允许延迟
                        socket.setSoTimeout(0); // 读取数据时的超时时间为0，没有超时，阻塞读取

                        // 读取对方传输过来的信息
                        RemoteServerNode remoteServerNode = readRemoteNodeInformation(socket);
                        if(remoteServerNode == null) {
                            fatal = true;
                            break;
                        }

                        // 为建立好的网络连接，启动IO线程
                        startServerIOThreads(remoteServerNode.getNodeId(), socket);
                        // 维护这个建立成功的连接
                        addRemoteNodeSocket(remoteServerNode.getNodeId(), socket);
                        // 添加建立连接的远程节点
                        addRemoteServerNode(remoteServerNode);

                        // 发送自己的信息过去给对方
                        if(!sendSelfInformation(socket)) {
                            fatal = true;
                            break;
                        }

                        LOGGER.info("连接监听线程已经跟远程server节点建立连接：" + remoteServerNode + ", IO线程全部启动......");
                    }
                } catch (IOException e) {
                    LOGGER.error("连接监听线程在监听连接请求的过程中发生异常！！！", e);

                    // 重试次数加1
                    this.retries++;
                    if(this.retries <= DEFAULT_RETRIES) {
                        LOGGER.error("本次是第" + retries + "次重试去监听连接请求......");
                    }
                } finally {
                    // 将ServerSocket进行关闭
                    try {
                        this.serverSocket.close();
                    } catch (IOException ex) {
                        LOGGER.error("关闭ServerSocket异常！！！", ex);
                    }
                }

                // 如果是遇到了系统不可逆的异常，直接崩溃
                if(fatal) {
                    break;
                }
            }

            // 在这里，就说明这个master节点无法监听其他节点的连接请求
            NodeStatus nodeStatus = NodeStatus.getInstance();
            nodeStatus.setStatus(NodeStatus.FATAL);

            LOGGER.error("无法正常监听其他server节点的连接请求，系统即将崩溃！！！");
        }

    }

    /**
     * 删除跟指定节点的网络连接数据
     * @param remoteNodeId
     */
    public void clearConnection(Integer remoteNodeId) {
        LOGGER.info("跟节点【" + remoteNodeId + "】的网络连接断开，清理相关数据......");

        remoteNodeSockets.remove(remoteNodeId);
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        remoteServerNodeManager.removeServerNode(remoteNodeId);

        IOThreadRunningSignal ioThreadRunningSignal =
                ioThreadRunningSignals.get(remoteNodeId);
        ioThreadRunningSignal.setIsRunning(false);

        ByteBuffer terminateBuffer = ByteBuffer.allocate(4);
        terminateBuffer.putInt(MessageType.TERMINATE);
        sendQueues.get(remoteNodeId).offer(terminateBuffer);

        sendQueues.remove(remoteNodeId);
    }

}
