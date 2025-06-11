package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;
import java.util.UUID;

public class SubscribeRequest extends Request {

    private SubscribeRequest() {

    }

    private String id;
    private String serviceName;
    private ByteBuffer data;

    public static class Builder {

        private SubscribeRequest request = new SubscribeRequest();

        public Builder() {
            String id = UUID.randomUUID().toString().replace(
                    "-", "");
            request.setId(id);
        }

        public SubscribeRequest.Builder serviceName(String serviceName) {
            this.request.setServiceName(serviceName);
            return this;
        }

        public SubscribeRequest build() {
            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    Request.REQUEST_FLAG_BYTES +
                    Request.REQUEST_LENGTH_BYTES +
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    this.request.getServiceName().length()
            );

            byteBuffer.putInt(Request.REQUEST_FLAG);
            byteBuffer.putInt(
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    this.request.getServiceName().length()
            );
            byteBuffer.putInt(Request.SUBSCRIBE);
            byteBuffer.put(this.request.getId().getBytes());
            byteBuffer.putInt(this.request.getServiceName().length());
            byteBuffer.put(this.request.getServiceName().getBytes());
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

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public static SubscribeRequest deserialize(ByteBuffer buffer) {
        byte[] idBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(idBytes);
        String id = new String(idBytes);

        Integer serviceNameLength = buffer.getInt();
        byte[] serviceNameBytes = new byte[serviceNameLength];
        buffer.get(serviceNameBytes);
        String serviceName = new String(serviceNameBytes);

        SubscribeRequest request = new SubscribeRequest();
        request.setId(id);
        request.setServiceName(serviceName);

        return request;
    }

}
