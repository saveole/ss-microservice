package org.zhss.govern.client;

public class RemoteSocketAddressTest {

    public static void main(String[] args) {
        String remoteSocketAddress = "/127.0.0.1:2559";
        String[] remoteSocketAddressSplited = remoteSocketAddress
                .replace("/", "").split(":");
        System.out.println(remoteSocketAddressSplited[0]);
        System.out.println(remoteSocketAddressSplited[1]);
    }

}
