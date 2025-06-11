package org.zhss.govern.client;

public class RouteSlotTest {

    public static void main(String[] args) {
        String serviceName = "DEFAULT";
        int hashCode = serviceName.hashCode() & Integer.MAX_VALUE;
        int slot = hashCode % 16384;
        System.out.println(slot);
    }

}
