package org.zhss.govern.client;

import java.util.UUID;

public class UUIDLengthTest {

    public static void main(String[] args) {
        String id = UUID.randomUUID().toString().replace("-", "");
        System.out.println(id.getBytes().length);
    }

}
