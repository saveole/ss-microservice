package org.zhss.govern.client;

import com.zhss.microservice.client.config.Configuration;
import com.zhss.microservice.client.network.Server;

import java.util.List;

public class ConfigurationTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = Configuration.getInstance();
        List<Server> servers = configuration.getControllerCandidates();
        System.out.println(servers);
    }

}
