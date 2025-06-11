package com.zhss.microservice.server.node.network;

import java.util.ArrayList;
import java.util.List;

/**
 * 客户端连接管理组件
 */
public class ClientConnectionManager {

    private ClientConnectionManager() {

    }

    static class Singleton {
        static ClientConnectionManager instance = new ClientConnectionManager();
    }

    public static ClientConnectionManager getInstance() {
        return Singleton.instance;
    }

    // 跟所有客户端建立的连接
    private List<ClientConnection> clientConnections =
            new ArrayList<ClientConnection>();

    /**
     * 添加一个客户端连接
     * @param clientConnection
     */
    public void addClientConnection(ClientConnection clientConnection) {
        this.clientConnections.add(clientConnection);
    }

}
