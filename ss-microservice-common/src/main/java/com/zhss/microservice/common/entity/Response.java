package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;

/**
 * 响应数据
 */
public abstract class Response implements Message {

    /**
     * 响应标识
     */
    public static final Integer RESPONSE_FLAG = 2;

    /**
     * 响应标识符字节数
     */
    public static final Integer RESPONSE_FLAG_BYTES = 4;
    /**
     * 响应长度的字节数
     */
    public static final Integer RESPONSE_LENGTH_BYTES = 4;
    /**
     * JSON字符串长度的字节数
     */
    public static final Integer JSON_LENGTH_BYTES = 4;
    /**
     * 整数类型的请求字段的字节数
     */
    public static final Integer RESPONSE_INTEGER_FIELD_BYTES = 4;

    /**
     * 获取请求id
     * @return
     */
    public abstract String getRequestId();

    /**
     * 反序列化响应
     * @param requestType
     * @param messageBuffer
     * @return
     */
    public static Response deserialize(Integer requestType, ByteBuffer messageBuffer) {
        Response response = null;

        if(requestType.equals(Request.FETCH_SLOTS_ALLOCATION)) {
            response = FetchSlotsAllocationResponse.deserialize(messageBuffer);
        } else if(requestType.equals(Request.FETCH_SERVER_ADDRESSES)) {
            response = FetchServerAddressesResponse.deserialize(messageBuffer);
        } else if(requestType.equals(Request.REGISTER)) {
            response = RegisterResponse.deserialize(messageBuffer);
        } else if(requestType.equals(Request.HEARTBEAT)) {
            response = HeartbeatResponse.deserialize(messageBuffer);
        } else if(requestType.equals(Request.FETCH_SERVER_NODE_ID)) {
            response = FetchServerNodeIdResponse.deserialize(messageBuffer);
        } else if(requestType.equals(Request.SUBSCRIBE)) {
            response = SubscribeResponse.deserialize(messageBuffer);
        } else if(requestType.equals(Request.SERVICE_CHANGED)) {
            response = ServiceChangedResponse.deserialize(messageBuffer);
        }

        return response;
    }

}
