package com.zhss.microservice.common.entity;

import com.alibaba.fastjson.JSONObject;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * 服务注册响应
 */
public class RegisterResponse extends Response {

    public static final Integer SUCCESS = 1;
    public static final Integer FAILURE = 2;

    private RegisterResponse() {

    }

    /**
     * 请求id
     */
    private String requestId;
    /**
     * 服务注册响应结果
     */
    private Integer result;
    /**
     * 二进制字节数据
     */
    private ByteBuffer data;

    public static class Builder {

        private RegisterResponse response = new RegisterResponse();

        public RegisterResponse.Builder requestId(String requestId) {
            this.response.setRequestId(requestId);
            return this;
        }

        public RegisterResponse.Builder result(Integer result) {
            this.response.setResult(result);
            return this;
        }

        public RegisterResponse build() {
            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    RESPONSE_FLAG_BYTES +
                    RESPONSE_LENGTH_BYTES +
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Response.RESPONSE_INTEGER_FIELD_BYTES
            );

            byteBuffer.putInt(RESPONSE_FLAG);
            byteBuffer.putInt(
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Response.RESPONSE_INTEGER_FIELD_BYTES
            );
            byteBuffer.putInt(Request.REGISTER);
            byteBuffer.put(response.getRequestId().getBytes());
            byteBuffer.putInt(response.getResult());
            byteBuffer.flip();

            response.setData(byteBuffer);

            return response;
        }

    }

    /**
     * 反序列化获取请求对象
     * @param buffer
     * @return
     */
    public static RegisterResponse deserialize(ByteBuffer buffer) {
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        Integer result = buffer.getInt();

        RegisterResponse response = new RegisterResponse();
        response.setRequestId(requestId);
        response.setResult(result);

        return response;
    }

    public String getRequestId() {
        return requestId;
    }

    private void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Integer getResult() {
        return result;
    }

    public void setResult(Integer result) {
        this.result = result;
    }

    public ByteBuffer getData() {
        return data;
    }

    private void setData(ByteBuffer data) {
        this.data = data;
    }
}
