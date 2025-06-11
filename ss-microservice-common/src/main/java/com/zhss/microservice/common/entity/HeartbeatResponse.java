package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;

/**
 * 心跳响应
 */
public class HeartbeatResponse extends Response {

    public static final Integer SUCCESS = 1;
    public static final Integer FAILURE = 2;

    private HeartbeatResponse() {

    }

    /**
     * 请求id
     */
    private String requestId;
    /**
     * 响应结果
     */
    private Integer result;
    /**
     * 二进制字节数据
     */
    private ByteBuffer data;

    public static class Builder {

        private HeartbeatResponse response = new HeartbeatResponse();

        public HeartbeatResponse.Builder requestId(String requestId) {
            this.response.setRequestId(requestId);
            return this;
        }

        public HeartbeatResponse.Builder result(Integer result) {
            this.response.setResult(result);
            return this;
        }

        public HeartbeatResponse build() {
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
            byteBuffer.putInt(Request.HEARTBEAT);
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
    public static HeartbeatResponse deserialize(ByteBuffer buffer) {
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        Integer result = buffer.getInt();

        HeartbeatResponse response = new HeartbeatResponse();
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
