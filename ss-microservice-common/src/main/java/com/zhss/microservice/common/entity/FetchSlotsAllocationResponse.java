package com.zhss.microservice.common.entity;

import com.alibaba.fastjson.JSONObject;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 拉取slots分配数据响应
 */
public class FetchSlotsAllocationResponse extends Response {

    private FetchSlotsAllocationResponse() {

    }

    /**
     * 请求id
     */
    private String requestId;
    /**
     * slots槽位分配数据
     */
    private Map<Integer, List<String>> slotsAllocation;
    /**
     * 二进制字节数据
     */
    private ByteBuffer data;

    /**
     * 抓取slots分配数据响应的构造器
     */
    public static class Builder {

        private FetchSlotsAllocationResponse fetchSlotsAllocationResponse =
                new FetchSlotsAllocationResponse();

        public FetchSlotsAllocationResponse.Builder requestId(String requestId) {
            this.fetchSlotsAllocationResponse.setRequestId(requestId);
            return this;
        }

        public FetchSlotsAllocationResponse.Builder slotsAllocation(
                Map<Integer, List<String>> slotsAllocation) {
            this.fetchSlotsAllocationResponse.setSlotsAllocation(slotsAllocation);
            return this;
        }

        public FetchSlotsAllocationResponse build() {
            String slotsAllocationJSON = JSONObject.toJSONString(
                    fetchSlotsAllocationResponse.getSlotsAllocation());
            byte[] slotsAllocationJSONBytes = slotsAllocationJSON.getBytes();

            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    RESPONSE_FLAG_BYTES +
                            RESPONSE_LENGTH_BYTES +
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                            JSON_LENGTH_BYTES +
                    slotsAllocationJSONBytes.length
            );

            byteBuffer.putInt(RESPONSE_FLAG);
            byteBuffer.putInt(
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                            JSON_LENGTH_BYTES +
                    slotsAllocationJSONBytes.length
            );
            byteBuffer.putInt(Request.FETCH_SLOTS_ALLOCATION);
            byteBuffer.put(fetchSlotsAllocationResponse.getRequestId().getBytes());
            byteBuffer.putInt(slotsAllocationJSONBytes.length);
            byteBuffer.put(slotsAllocationJSONBytes);
            byteBuffer.flip();

            fetchSlotsAllocationResponse.setData(byteBuffer);

            return fetchSlotsAllocationResponse;
        }

    }

    /**
     * 反序列化获取请求对象
     * @param buffer
     * @return
     */
    public static FetchSlotsAllocationResponse deserialize(ByteBuffer buffer) {
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        int slotsAllocationJSONLength = buffer.getInt();
        byte[] slotsAllocationJSONBytes = new byte[slotsAllocationJSONLength];
        buffer.get(slotsAllocationJSONBytes);

        String slotsAllocationJSON = new String(slotsAllocationJSONBytes);
        Map<Integer, List<String>> slotsAllocation = JSONObject.parseObject(slotsAllocationJSON, HashMap.class);

        FetchSlotsAllocationResponse response = new FetchSlotsAllocationResponse();
        response.setRequestId(requestId);
        response.setSlotsAllocation(slotsAllocation);

        return response;
    }

    public String getRequestId() {
        return requestId;
    }

    private void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Map<Integer, List<String>> getSlotsAllocation() {
        return slotsAllocation;
    }

    private void setSlotsAllocation(Map<Integer, List<String>> slotsAllocation) {
        this.slotsAllocation = slotsAllocation;
    }

    public ByteBuffer getData() {
        return data;
    }

    private void setData(ByteBuffer data) {
        this.data = data;
    }
}
