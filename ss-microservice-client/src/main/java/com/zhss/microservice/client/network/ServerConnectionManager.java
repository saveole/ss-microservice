package com.zhss.microservice.client.network;

import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 跟服务端连接的管理组件
 */
public class ServerConnectionManager {

    /**
     * 跟所有服务端建立的长连接
     */
    public Map<String, ServerConnection> serverConnections =
            new ConcurrentHashMap<>();

    /**
     * 添加一个跟远程server的连接
     * @param serverConnection
     */
    public void addServerConnection(ServerConnection serverConnection) {
        Socket socket = serverConnection.getSocketChannel().socket();
        String remoteSocketAddress = socket.getRemoteSocketAddress()
                .toString().replace("/", "");
        serverConnections.put(remoteSocketAddress, serverConnection);
    }

    /**
     * 获取跟指定server地址建立的连接
     * @param remoteSocketAddress
     * @return
     */
    public ServerConnection getServerConnection(String remoteSocketAddress) {
        return serverConnections.get(remoteSocketAddress);
    }

    /**
     * 判断server地址是否建立过连接
     * @param server
     * @return
     */
    public Boolean hasConnected(Server server) {
        return serverConnections.containsKey(server.getRemoteSocketAddress());
    }

}
