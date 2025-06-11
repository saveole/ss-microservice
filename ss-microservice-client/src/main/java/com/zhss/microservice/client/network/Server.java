package com.zhss.microservice.client.network;

/**
 * 代表了一个server节点
 */
public class Server {

    /**
     * 节点id
     */
    private Integer id;
    /**
     * ip/hostname地址
     */
    private String address;
    /**
     * 端口号
     */
    private int port;

    public Server(Integer id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    public Server(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getRemoteSocketAddress() {
        return getAddress() + ":" + getPort();
    }

    @Override
    public String toString() {
        return "Server{" +
                "id=" + id +
                ", address='" + address + '\'' +
                ", port=" + port +
                '}';
    }

}
