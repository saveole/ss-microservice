package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;

public class ServiceChangedResponse extends Response {

    public static final Integer SUCCESS = 1;
    public static final Integer FAILURE = 2;

    private ServiceChangedResponse() {

    }

    private String requestId;
    private Integer result;
    private ByteBuffer data;

    public static class Builder {

        private ServiceChangedResponse response = new ServiceChangedResponse();

        public ServiceChangedResponse.Builder requestId(String requestId) {
            this.response.setRequestId(requestId);
            return this;
        }

        public ServiceChangedResponse.Builder result(Integer result) {
            this.response.setResult(result);
            return this;
        }

        public ServiceChangedResponse build() {
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
            byteBuffer.putInt(Request.SERVICE_CHANGED);
            byteBuffer.put(response.getRequestId().getBytes());
            byteBuffer.putInt(response.getResult());
            byteBuffer.flip();

            response.setData(byteBuffer);

            return response;
        }

    }

    public static ServiceChangedResponse deserialize(ByteBuffer buffer) {
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        Integer result = buffer.getInt();

        ServiceChangedResponse response = new ServiceChangedResponse();
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
