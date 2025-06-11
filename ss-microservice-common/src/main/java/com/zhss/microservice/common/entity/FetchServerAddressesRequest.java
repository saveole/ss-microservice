package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * 拉取slots分配数据请求
 */
public class FetchServerAddressesRequest extends Request {

    private FetchServerAddressesRequest() {

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

        private FetchServerAddressesRequest request =
                new FetchServerAddressesRequest();

        public FetchServerAddressesRequest build() {
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
            byteBuffer.putInt(Request.FETCH_SERVER_ADDRESSES);
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
    public static FetchServerAddressesRequest deserialize(ByteBuffer buffer) {
        // 解析请求id
        byte[] requestIdBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(requestIdBytes);
        String requestId = new String(requestIdBytes);

        FetchServerAddressesRequest request = new FetchServerAddressesRequest();
        request.setId(requestId);

        return request;
    }

}
