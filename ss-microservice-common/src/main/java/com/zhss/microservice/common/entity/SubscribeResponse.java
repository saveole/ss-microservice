package com.zhss.microservice.common.entity;

import com.alibaba.fastjson.JSONObject;

import java.nio.ByteBuffer;
import java.util.List;

public class SubscribeResponse extends Response {

    private SubscribeResponse() {

    }

    private String requestId;
    private List<String> serviceInstanceAddresses;
    private ByteBuffer data;

    public static class Builder {

        private SubscribeResponse response = new SubscribeResponse();

        public SubscribeResponse.Builder requestId(String requestId) {
            this.response.setRequestId(requestId);
            return this;
        }

        public SubscribeResponse.Builder serviceInstanceAddresses(
                List<String> serviceInstanceAddresses) {
            this.response.setServiceInstanceAddresses(serviceInstanceAddresses);
            return this;
        }

        public SubscribeResponse build() {
            String serviceInstanceAddressesJson =
                    JSONObject.toJSONString(this.response.getServiceInstanceAddresses());
            byte[] serviceInstanceAddressesBytes = serviceInstanceAddressesJson.getBytes();

            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    RESPONSE_FLAG_BYTES +
                    RESPONSE_LENGTH_BYTES +
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Response.JSON_LENGTH_BYTES +
                    serviceInstanceAddressesBytes.length
            );

            byteBuffer.putInt(RESPONSE_FLAG);
            byteBuffer.putInt(
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Response.JSON_LENGTH_BYTES +
                    serviceInstanceAddressesBytes.length
            );
            byteBuffer.putInt(Request.SUBSCRIBE);
            byteBuffer.put(response.getRequestId().getBytes());
            byteBuffer.putInt(serviceInstanceAddressesBytes.length);
            byteBuffer.put(serviceInstanceAddressesBytes);
            byteBuffer.flip();

            response.setData(byteBuffer);

            return response;
        }

    }

    public static SubscribeResponse deserialize(ByteBuffer buffer) {
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        byte[] serviceInstanceAddressesBytes = new byte[buffer.getInt()];
        buffer.get(serviceInstanceAddressesBytes);
        String serviceInstanceAddressesJson = new String(serviceInstanceAddressesBytes);
        List<String> serviceInstanceAddresses = JSONObject.parseObject(
                serviceInstanceAddressesJson, List.class);

        SubscribeResponse response = new SubscribeResponse();
        response.setRequestId(requestId);
        response.setServiceInstanceAddresses(serviceInstanceAddresses);

        return response;
    }

    public String getRequestId() {
        return requestId;
    }

    private void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<String> getServiceInstanceAddresses() {
        return serviceInstanceAddresses;
    }

    public void setServiceInstanceAddresses(List<String> serviceInstanceAddresses) {
        this.serviceInstanceAddresses = serviceInstanceAddresses;
    }

    public ByteBuffer getData() {
        return data;
    }

    private void setData(ByteBuffer data) {
        this.data = data;
    }
}
