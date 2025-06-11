package com.zhss.microservice.client.core;

import com.zhss.microservice.client.network.ServerRequestProcessor;
import com.zhss.microservice.common.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.client.config.Configuration;
import com.zhss.microservice.client.network.Server;
import com.zhss.microservice.client.network.ServerConnection;
import com.zhss.microservice.client.network.ServerConnectionManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 代表服务实例的一个客户端
 */
public class ServiceInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceInstance.class);
    private static final Long SELECTOR_TIMEOUT = 5000L;
    private static final Long REQUEST_WAIT_SLEEP_INTERVAL = 10L;
    private static final Integer SLOT_COUNT = 16384;

    /**
     * NIO多路复用组件
     */
    private Selector selector;
    /**
     * 服务端连接的管理组件
     */
    private ServerConnectionManager serverConnectionManager;
    /**
     * 发送出去的请求以及对应的响应
     * bug2：每个请求的响应在收到处理完毕之后，需要从map里删除掉，避免内存泄漏
     */
    private ConcurrentHashMap<String, Response> responses =
            new ConcurrentHashMap<String, Response>();
    /**
     * 槽位分配数据
     */
    private Map<Integer, List<String>> slotsAllocation;
    /**
     * server地址列表
     */
    private Map<Integer, Server> servers = new HashMap<Integer, Server>();
    /**
     * 自己连接的controller候选节点
     */
    private ServerConnection controllerCandidateConnection;
    /**
     * 自己服务实例所路由到的server的连接
     */
    private ServerConnection serverConnection;
    private Server server;

    /**
     * 构造函数
     */
    public ServiceInstance() {
        try {
            this.selector = Selector.open();
            this.serverConnectionManager = new ServerConnectionManager();
            new NetworkIOThread().start();
        } catch (IOException e) {
            LOGGER.error("open selector error......", e);
        }
    }

    /**
     * 进行服务实例的初始化
     * @throws IOException
     * @throws InterruptedException
     */
    public void init() throws Exception {
        // 从controller候选节点拉取槽位分配数据、server地址列表
        Server controllerCandidate = chooseControllerCandidate();
        this.controllerCandidateConnection = connectServer(controllerCandidate);
        controllerCandidate.setId(fetchServerNodeId(controllerCandidate));
        fetchSlotsAllocation(controllerCandidate);
        fetchServerAddresses(controllerCandidate);

        // bug1：已经连接的controller候选几点就是你要路由的那个节点，此时不应该重复连接

        // 将服务实例路由到server节点，完成跟server节点的连接
        String serviceName = Configuration.getInstance().getServiceName();
        this.server = routeServer(serviceName);
        // 有一个判断，判断你刚才连接的controller候选节点的id和你的路由server的id是否一致
        // 如果是一致的，此时就不用重复连接那个server了
        if(controllerCandidate.getId().equals(server.getId())) {
            this.serverConnection = this.controllerCandidateConnection;
        } else {
            this.serverConnection = connectServer(server);
        }
    }

    /**
     * 拉取server node id
     * @param controllerCandidate
     * @return
     */
    private Integer fetchServerNodeId(Server controllerCandidate)
            throws Exception {
        FetchServerNodeIdRequest request = new FetchServerNodeIdRequest.Builder().build();
        FetchServerNodeIdResponse response = (FetchServerNodeIdResponse)
                sendRequest(request, controllerCandidate);
        return response.getServerNodeId();
    }

    /**
     * 服务实例的注册
     */
    public Boolean register() throws Exception {
        // 提取服务实例的配置信息
        Configuration configuration = Configuration.getInstance();
        String serviceName = configuration.getServiceName();
        String serviceInstanceIp = configuration.getServiceInstanceIp();
        Integer serviceInstancePort = configuration.getServiceInstancePort();

        // 构建服务注册请求
        RegisterRequest.Builder requestBuilder = new RegisterRequest.Builder();
        RegisterRequest request = requestBuilder
                .serviceName(serviceName)
                .serviceInstanceIp(serviceInstanceIp)
                .serviceInstancePort(serviceInstancePort)
                .build();

        // 将服务注册请求发送到自己slot所在的server上去
        ServerMessageQueues serverMessageQueues = ServerMessageQueues.getInstance();
        serverMessageQueues.offer(serverConnection.getConnectionId(), request);

        LOGGER.info("准备发送服务注册请求，开始等待服务注册的响应结果......");

        // server端处理服务注册请求，返回响应
        // 等待服务注册响应的返回
        while(responses.get(request.getId()) == null) {
            Thread.sleep(REQUEST_WAIT_SLEEP_INTERVAL);
        }

        LOGGER.info("服务注册已经成功......");
        responses.remove(request.getId());

        return true;
    }

    /**
     * 启动定时发送心跳的线程
     */
    public void startHeartbeatScheduler() {
        new HeartbeatThread().start();
    }

    /**
     * 服务订阅
     * 1、可以返回给我指定服务的所有实例的列表
     * 2、指定的服务如果后续有实例列表的变动，则可以主动的告诉我这个客户端，他的变动情况
     */
    public List<ServiceInstanceAddress> subscribe(String serviceName) throws Exception {
        // 看一下，要订阅的服务名称是否已经订阅过了
        CachedServiceRegistry cachedServiceRegistry = CachedServiceRegistry.getInstance();
        if(cachedServiceRegistry.isCached(serviceName)) {
            return cachedServiceRegistry.get(serviceName);
        }

        // 我们得先找到你要订阅的服务所在的server
        Server server = routeServer(serviceName);
        // 判断一下对这个server是否已经完成连接了，如果没连接，那么就先连接
        if(!serverConnectionManager.hasConnected(server)) {
            connectServer(server);
        }
        LOGGER.info(serviceName + "服务在server上：" + server + "，准备发送订阅请求......");

        // 向指定的server发送过去subscribe请求
        SubscribeRequest request = new SubscribeRequest.Builder()
                .serviceName(serviceName)
                .build();
        SubscribeResponse response = (SubscribeResponse) sendRequest(request, server);

        // 对服务实例地址的结果进行处理
        List<ServiceInstanceAddress> serviceInstanceAddresses = new ArrayList<ServiceInstanceAddress>();

        for(String serviceInstanceAddressInfo : response.getServiceInstanceAddresses()) {
            String[] serviceInstanceAddressInfoSplited =
                    serviceInstanceAddressInfo.split(",");

            ServiceInstanceAddress serviceInstanceAddress = new ServiceInstanceAddress(
                    serviceInstanceAddressInfoSplited[0],
                    serviceInstanceAddressInfoSplited[1],
                    Integer.parseInt(serviceInstanceAddressInfoSplited[2])
            );

            serviceInstanceAddresses.add(serviceInstanceAddress);
        }

        // 对服务实例地址进行本地缓存
        cachedServiceRegistry.cache(serviceName, serviceInstanceAddresses);
        LOGGER.info("获取到服务【" + serviceName + "】的最新实例地址列表：" + serviceInstanceAddresses);

        return serviceInstanceAddresses;
    }

    /**
     * 获取服务名称下最新的服务实例地址列表
     * @param serviceName
     * @return
     */
    public List<ServiceInstanceAddress> getServiceInstanceAddresses(String serviceName) {
        CachedServiceRegistry cachedServiceRegistry = CachedServiceRegistry.getInstance();
        return cachedServiceRegistry.get(serviceName);
    }

    /**
     * 发送请求到指定的server去
     * @param request
     * @param server
     */
    private Response sendRequest(Request request, Server server) throws Exception {
        ServerConnection serverConnection = serverConnectionManager
                .getServerConnection(server.getRemoteSocketAddress());

        ServerMessageQueues serverMessageQueues = ServerMessageQueues.getInstance();
        serverMessageQueues.offer(serverConnection.getConnectionId(), request);

        while(responses.get(request.getId()) == null) {
            Thread.sleep(REQUEST_WAIT_SLEEP_INTERVAL);
        }

        Response response = responses.get(request.getId());
        responses.remove(request.getId());

        return response;
    }

    /**
     * 将服务实例路由到一个server节点
     */
    private Server routeServer(String serviceName) {
        Integer slot = routeSlot(serviceName);
        Integer serverId = locateServerBySlot(slot);
        Server server = servers.get(serverId);
        LOGGER.info("服务实例路由到server节点：" + server);
        return server;
    }

    /**
     * 把服务路由到一个槽位
     * @return
     */
    private Integer routeSlot(String serviceName) {
        int hashCode = serviceName.hashCode() & Integer.MAX_VALUE;
        Integer slot = hashCode % SLOT_COUNT;

        if(slot == 0) {
            slot = slot + 1;
        }

        return slot;
    }

    /**
     * 根据槽位定位master节点
     * @param slot
     * @return
     */
    private Integer locateServerBySlot(Integer slot) {
        for(Integer serverNodeId : slotsAllocation.keySet()) {
            List<String> slotsList = slotsAllocation.get(serverNodeId);

            for(String slots : slotsList) {
                String[] slotsSpited = slots.split(",");
                Integer startSlot = Integer.valueOf(slotsSpited[0]);
                Integer endSlot = Integer.valueOf(slotsSpited[1]);

                if(slot >= startSlot && slot <= endSlot) {
                    return serverNodeId;
                }
            }
        }
        return null;
    }

    /**
     * 随机挑选一个controller候选节点
     * @return
     */
    private Server chooseControllerCandidate() {
        Configuration configuration = Configuration.getInstance();
        List<Server> controllerCandidates = configuration.getControllerCandidates();

        Random random = new Random();
        int index = random.nextInt(controllerCandidates.size());
        Server server = controllerCandidates.get(index);

        return server;
    }

    /**
     * 跟指定的server建立长连接
     * @param server
     */
    private ServerConnection connectServer(Server server) throws IOException {
        // 向server节点发起连接请求
        InetSocketAddress address = new InetSocketAddress(
                server.getAddress(), server.getPort());

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.socket().setSoLinger(false, -1);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);

        socketChannel.connect(address);
        LOGGER.info("正在尝试连接到server节点: " + server);

        // 等待跟server节点建立连接
        String remoteSocketAddress = server.getRemoteSocketAddress();
        ServerConnection serverConnection = null;
        boolean finishedConnect = false;

        while(!finishedConnect) {
            serverConnection = serverConnectionManager
                    .getServerConnection(remoteSocketAddress);
            if(serverConnection != null) {
                finishedConnect = true;
            }
        }

        return serverConnection;
    }

    /**
     * 发送请求到server，拉取slots分配数据
     */
    private void fetchSlotsAllocation(Server controllerCandidate) throws Exception {
        FetchSlotsAllocationRequest request =
                new FetchSlotsAllocationRequest.Builder().build();
        FetchSlotsAllocationResponse response = (FetchSlotsAllocationResponse)
                sendRequest(request, controllerCandidate);
        this.slotsAllocation = response.getSlotsAllocation();
        LOGGER.info("拉取到槽位分配数据: " + slotsAllocation);
    }

    /**
     * 拉取server节点地址列表
     * @param controllerCandidate
     * @throws IOException
     * @throws InterruptedException
     */
    private void fetchServerAddresses(Server controllerCandidate) throws Exception {
        FetchServerAddressesRequest request =
                new FetchServerAddressesRequest.Builder().build();
        FetchServerAddressesResponse response = (FetchServerAddressesResponse)
                sendRequest(request, controllerCandidate);

        List<String> serverAddresses = response.getServerAddresses();

        for(String serverAddress : serverAddresses) {
            String[] serverAddressSplited =  serverAddress.split(":");

            Integer id = Integer.valueOf(serverAddressSplited[0]);
            String ip = serverAddressSplited[1];
            Integer port = Integer.valueOf(serverAddressSplited[2]);
            Server server = new Server(id, ip, port);

            servers.put(id, server);
        }

        LOGGER.info("拉取到server地址列表: " + servers);
    }

    /**
     * 定时心跳线程
     */
    class HeartbeatThread extends Thread {

        @Override
        public void run() {
            // 提取需要的配置信息
            Configuration configuration = Configuration.getInstance();

            Integer heartbeatInterval = configuration.getHeartbeatInterval();
            String serviceName = configuration.getServiceName();
            String serviceInstanceIp = configuration.getServiceInstanceIp();
            Integer serviceInstancePort = configuration.getServiceInstancePort();

            while(true) {
                try {
                    HeartbeatRequest.Builder requestBuilder = new HeartbeatRequest.Builder();
                    HeartbeatRequest request = requestBuilder
                            .serviceName(serviceName)
                            .serviceInstanceIp(serviceInstanceIp)
                            .serviceInstancePort(serviceInstancePort)
                            .build();

                    sendRequest(request, server);

                    System.out.println("发送心跳......");

                    Thread.sleep(heartbeatInterval * 1000);
                } catch(Exception e) {
                    LOGGER.error("定时心跳线程被异常中断！！！", e);
                }
            }
        }

    }

    /**
     * 负责网络IO的线程
     */
    class NetworkIOThread extends Thread {

        @Override
        public void run() {
            // 把请求数据发送到server端
            // 解决发送请求时候可能出现的拆包的问题

            while(true) {
                try {
                    selector.select(SELECTOR_TIMEOUT);

                    Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    if(selectionKeys == null || selectionKeys.isEmpty()) {
                        continue;
                    }

                    for(SelectionKey selectionKey : selectionKeys) {
                        // 处理跟server端的连接
                        if((selectionKey.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                            if(selectionKey.isConnectable()) {
                                SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                                if(socketChannel.finishConnect()) {
                                    selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);

                                    serverConnection = new ServerConnection(selectionKey, socketChannel);

                                    ServerMessageQueues serverMessageQueues = ServerMessageQueues.getInstance();
                                    serverMessageQueues.init(serverConnection.getConnectionId());

                                    selectionKey.attach(serverConnection);

                                    serverConnectionManager.addServerConnection(serverConnection);

                                    LOGGER.info("已经跟server节点建立连接：" + socketChannel.socket() +  "......");
                                }
                            }
                        }
                        // 读取服务端返回的响应
                        if((selectionKey.readyOps() & SelectionKey.OP_READ) != 0) {
                            if(selectionKey.isReadable()) {
                                ServerConnection serverConnection = (ServerConnection)
                                        selectionKey.attachment();

                                Integer messageFlag = serverConnection.readMessageFlag();

                                if(messageFlag != null) {
                                    Message message = serverConnection.readMessage();
                                    if (message != null) {
                                        if(message instanceof Response) {
                                            Response response = (Response) message;
                                            responses.put(response.getRequestId(), response);
                                        } else if(message instanceof Request) {
                                            Request request = (Request) message;
                                            ServerRequestProcessor serverRequestProcessor = ServerRequestProcessor.getInstance();
                                            Response response = serverRequestProcessor.process(request);

                                            ServerMessageQueues serverMessageQueues = ServerMessageQueues.getInstance();
                                            serverMessageQueues.offer(serverConnection.getConnectionId(), response);
                                        }
                                    }
                                }
                            }
                        }
                        // 发送请求/响应给服务端
                        if ((selectionKey.readyOps() & SelectionKey.OP_WRITE) != 0) {
                            if(selectionKey.isWritable()) {
                                ServerConnection serverConnection = (ServerConnection)
                                        selectionKey.attachment();
                                if(serverConnection == null) {
                                    continue;
                                }

                                ServerMessageQueues serverMessageQueues = ServerMessageQueues.getInstance();
                                LinkedBlockingQueue<Message> messageQueue =
                                        serverMessageQueues.get(serverConnection.getConnectionId());
                                if(messageQueue.isEmpty()) {
                                    continue;
                                }

                                Message message = messageQueue.peek();
                                if(message == null) {
                                    continue;
                                }

                                ByteBuffer requestData = message.getData();

                                SocketChannel socketChannel = serverConnection.getSocketChannel();
                                socketChannel.write(requestData);

                                if(!requestData.hasRemaining()) {
                                    messageQueue.poll();
                                }
                            }
                        }

                    }
                } catch(Exception e) {
                    LOGGER.error("client network io thread error......", e);
                }
            }
        }
    }

}
