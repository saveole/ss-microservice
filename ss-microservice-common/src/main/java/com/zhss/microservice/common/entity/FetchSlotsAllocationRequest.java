package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * 拉取slots分配数据请求
 */
public class FetchSlotsAllocationRequest extends Request {

    private FetchSlotsAllocationRequest() {

    }

    /**
     * 请求id
     */
    private String id;
    /**
     * 请求数据
     */
    private ByteBuffer data;

    public static class Builder {

        private FetchSlotsAllocationRequest fetchSlotsAllocationRequest =
                new FetchSlotsAllocationRequest();

        public FetchSlotsAllocationRequest build() {
            String id = UUID.randomUUID().toString().replace("-", "");
            fetchSlotsAllocationRequest.setId(id);

            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    Request.REQUEST_FLAG_BYTES +
                    Request.REQUEST_LENGTH_BYTES +
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES
            );

            byteBuffer.putInt(Request.REQUEST_FLAG);
            byteBuffer.putInt(
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES
            );
            byteBuffer.putInt(Request.FETCH_SLOTS_ALLOCATION);
            byteBuffer.put(id.getBytes());
            byteBuffer.flip();

            fetchSlotsAllocationRequest.setData(byteBuffer);

            return fetchSlotsAllocationRequest;
        }

    }

    public ByteBuffer getData() {
        return data;
    }

    public String getId() {
        return id;
    }

    private void setData(ByteBuffer data) {
        this.data = data;
    }

    private void setId(String id) {
        this.id = id;
    }

    /**
     * 反序列化获取请求对象
     * @param buffer
     * @return
     */
    public static FetchSlotsAllocationRequest deserialize(ByteBuffer buffer) {
        // 解析请求id
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        FetchSlotsAllocationRequest request = new FetchSlotsAllocationRequest();
        request.setId(requestId);

        return request;
    }

}
