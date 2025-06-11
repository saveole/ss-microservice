package com.zhss.microservice.common.entity;

import com.alibaba.fastjson.JSONObject;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

public class ServiceChangedRequest extends Request {

    private ServiceChangedRequest() {

    }

    private String id;
    private String serviceName;
    private List<String> serviceInstanceAddresses;
    private ByteBuffer data;

    public static class Builder {

        private ServiceChangedRequest request = new ServiceChangedRequest();

        public Builder() {
            String id = UUID.randomUUID().toString().replace(
                    "-", "");
            request.setId(id);
        }

        public ServiceChangedRequest.Builder serviceName(String serviceName) {
            this.request.setServiceName(serviceName);
            return this;
        }

        public ServiceChangedRequest.Builder serviceInstanceAddresses(List<String> serviceInstanceAddresses) {
            this.request.setServiceInstanceAddresses(serviceInstanceAddresses);
            return this;
        }

        public ServiceChangedRequest build() {
            byte[] serviceInstancesAddressesBytes = JSONObject.toJSONString(
                    request.getServiceInstanceAddresses()).getBytes();

            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    Request.REQUEST_FLAG_BYTES +
                    Request.REQUEST_LENGTH_BYTES +
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    this.request.getServiceName().length() +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    serviceInstancesAddressesBytes.length
            );

            byteBuffer.putInt(Request.REQUEST_FLAG);
            byteBuffer.putInt(
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    this.request.getServiceName().length() +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    serviceInstancesAddressesBytes.length
            );
            byteBuffer.putInt(Request.SERVICE_CHANGED);
            byteBuffer.put(this.request.getId().getBytes());
            byteBuffer.putInt(this.request.getServiceName().length());
            byteBuffer.put(this.request.getServiceName().getBytes());
            byteBuffer.putInt(serviceInstancesAddressesBytes.length);
            byteBuffer.put(serviceInstancesAddressesBytes);
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

    public List<String> getServiceInstanceAddresses() {
        return serviceInstanceAddresses;
    }

    public void setServiceInstanceAddresses(List<String> serviceInstanceAddresses) {
        this.serviceInstanceAddresses = serviceInstanceAddresses;
    }

    public static ServiceChangedRequest deserialize(ByteBuffer buffer) {
        byte[] idBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(idBytes);
        String id = new String(idBytes);

        Integer serviceNameLength = buffer.getInt();
        byte[] serviceNameBytes = new byte[serviceNameLength];
        buffer.get(serviceNameBytes);
        String serviceName = new String(serviceNameBytes);

        Integer serviceInstanceAddressesBytesLength = buffer.getInt();
        byte[] serviceInstanceAddressesBytes = new byte[serviceInstanceAddressesBytesLength];
        buffer.get(serviceInstanceAddressesBytes);
        List<String> serviceInstanceAddresses = JSONObject.parseObject(
                new String(serviceInstanceAddressesBytes), List.class);

        ServiceChangedRequest request = new ServiceChangedRequest();
        request.setId(id);
        request.setServiceName(serviceName);
        request.setServiceInstanceAddresses(serviceInstanceAddresses);

        return request;
    }

}
