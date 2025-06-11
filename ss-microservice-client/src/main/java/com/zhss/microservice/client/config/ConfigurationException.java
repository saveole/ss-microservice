package com.zhss.microservice.client.config;

/**
 * 配置相关的异常
 */
public class ConfigurationException extends Exception {

    public ConfigurationException(String msg) {
        super(msg);
    }

    public ConfigurationException(String msg, Exception e) {
        super(msg, e);
    }

}
