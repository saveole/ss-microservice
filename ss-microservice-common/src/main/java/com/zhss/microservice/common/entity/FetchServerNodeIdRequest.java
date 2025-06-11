package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;
import java.util.UUID;

public class FetchServerNodeIdRequest extends Request {

    private FetchServerNodeIdRequest() {

    }

    private String id;
    private ByteBuffer data;

    public static class Builder {

        private FetchServerNodeIdRequest request =
                new FetchServerNodeIdRequest();

        public FetchServerNodeIdRequest build() {
            String id = UUID.randomUUID().toString().replace("-", "");
            request.setId(id);

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
            byteBuffer.putInt(Request.FETCH_SERVER_NODE_ID);
            byteBuffer.put(id.getBytes());
            byteBuffer.flip();

            request.setData(byteBuffer);

            return request;
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
    public static FetchServerNodeIdRequest deserialize(ByteBuffer buffer) {
        // 解析请求id
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        FetchServerNodeIdRequest request = new FetchServerNodeIdRequest();
        request.setId(requestId);

        return request;
    }

}
