package com.zhss.microservice.common.entity;

import com.alibaba.fastjson.JSONObject;

import java.nio.ByteBuffer;
import java.util.List;

public class FetchServerNodeIdResponse extends Response {

    private FetchServerNodeIdResponse() {

    }

    private String requestId;
    private Integer serverNodeId;
    private ByteBuffer data;

    public static class Builder {

        private FetchServerNodeIdResponse response =
                new FetchServerNodeIdResponse();

        public FetchServerNodeIdResponse.Builder requestId(String requestId) {
            this.response.setRequestId(requestId);
            return this;
        }

        public FetchServerNodeIdResponse.Builder serverNodeId(
                Integer serverNodeId) {
            this.response.setServerNodeId(serverNodeId);
            return this;
        }

        public FetchServerNodeIdResponse build() {
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
            byteBuffer.putInt(Request.FETCH_SERVER_NODE_ID);
            byteBuffer.put(response.getRequestId().getBytes());
            byteBuffer.putInt(response.getServerNodeId());
            byteBuffer.flip();

            response.setData(byteBuffer);

            return response;
        }

    }

    public static FetchServerNodeIdResponse deserialize(ByteBuffer buffer) {
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        Integer serverNodeId = buffer.getInt();

        FetchServerNodeIdResponse response = new FetchServerNodeIdResponse();
        response.setRequestId(requestId);
        response.setServerNodeId(serverNodeId);

        return response;
    }

    public String getRequestId() {
        return requestId;
    }

    private void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Integer getServerNodeId() {
        return serverNodeId;
    }

    public void setServerNodeId(Integer serverNodeId) {
        this.serverNodeId = serverNodeId;
    }

    public ByteBuffer getData() {
        return data;
    }

    private void setData(ByteBuffer data) {
        this.data = data;
    }
}
