package com.zhss.microservice.common.entity;

import com.alibaba.fastjson.JSONObject;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 拉取slots分配数据响应
 */
public class FetchServerAddressesResponse extends Response {

    private FetchServerAddressesResponse() {

    }

    /**
     * 请求id
     */
    private String requestId;
    /**
     * server地址列表
     */
    private List<String> serverAddresses;
    /**
     * 二进制字节数据
     */
    private ByteBuffer data;

    /**
     * 抓取slots分配数据响应的构造器
     */
    public static class Builder {

        private FetchServerAddressesResponse response =
                new FetchServerAddressesResponse();

        public FetchServerAddressesResponse.Builder requestId(String requestId) {
            this.response.setRequestId(requestId);
            return this;
        }

        public FetchServerAddressesResponse.Builder serverAddresses(
                List<String> serverAddresses) {
            this.response.setServerAddresses(serverAddresses);
            return this;
        }

        public FetchServerAddressesResponse build() {
            String json = JSONObject.toJSONString(response.getServerAddresses());
            byte[] jsonBytes = json.getBytes();

            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    RESPONSE_FLAG_BYTES +
                    RESPONSE_LENGTH_BYTES +
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    JSON_LENGTH_BYTES +
                    jsonBytes.length
            );

            byteBuffer.putInt(RESPONSE_FLAG);
            byteBuffer.putInt(
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    JSON_LENGTH_BYTES +
                    jsonBytes.length
            );
            byteBuffer.putInt(Request.FETCH_SERVER_ADDRESSES);
            byteBuffer.put(response.getRequestId().getBytes());
            byteBuffer.putInt(jsonBytes.length);
            byteBuffer.put(jsonBytes);
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
    public static FetchServerAddressesResponse deserialize(ByteBuffer buffer) {
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        int jsonLength = buffer.getInt();
        byte[] jsonBytes = new byte[jsonLength];
        buffer.get(jsonBytes);

        String json = new String(jsonBytes);
        List<String> serverAddresses = JSONObject.parseObject(json, List.class);

        FetchServerAddressesResponse response = new FetchServerAddressesResponse();
        response.setRequestId(requestId);
        response.setServerAddresses(serverAddresses);

        return response;
    }

    public String getRequestId() {
        return requestId;
    }

    private void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<String> getServerAddresses() {
        return serverAddresses;
    }

    public void setServerAddresses(List<String> serverAddresses) {
        this.serverAddresses = serverAddresses;
    }

    public ByteBuffer getData() {
        return data;
    }

    private void setData(ByteBuffer data) {
        this.data = data;
    }
}
