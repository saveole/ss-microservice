package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;

/**
 * 基础消息接口
 */
public interface Message {

    ByteBuffer getData();

}
