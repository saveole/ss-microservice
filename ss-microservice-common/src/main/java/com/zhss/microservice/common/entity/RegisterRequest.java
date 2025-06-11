package com.zhss.microservice.common.entity;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * 服务注册请求
 */
public class RegisterRequest extends Request {

    private RegisterRequest() {

    }

    /**
     * 请求id
     */
    private String id;
    /**
     * 服务名称
     */
    private String serviceName;
    /**
     * 服务实例的ip地址
     */
    private String serviceInstanceIp;
    /**
     * 服务实例的端口号
     */
    private Integer serviceInstancePort;
    /**
     * 请求字节数据
     */
    private ByteBuffer data;

    public static class Builder {

        private RegisterRequest request = new RegisterRequest();

        public Builder() {
            String id = UUID.randomUUID().toString().replace(
                    "-", "");
            request.setId(id);
        }

        public RegisterRequest.Builder serviceName(String serviceName) {
            this.request.setServiceName(serviceName);
            return this;
        }

        public RegisterRequest.Builder serviceInstanceIp(String serviceInstanceIp) {
            this.request.setServiceInstanceIp(serviceInstanceIp);
            return this;
        }

        public RegisterRequest.Builder serviceInstancePort(Integer serviceInstancePort) {
            this.request.setServiceInstancePort(serviceInstancePort);
            return this;
        }

        public RegisterRequest build() {
            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    Request.REQUEST_FLAG_BYTES +
                    Request.REQUEST_LENGTH_BYTES +
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    this.request.getServiceName().length() +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    this.request.getServiceInstanceIp().length() +
                    Request.REQUEST_INTEGER_FIELD_BYTES
            );

            byteBuffer.putInt(Request.REQUEST_FLAG);
            byteBuffer.putInt(
                    Request.REQUEST_TYPE_BYTES +
                    Request.REQUEST_ID_BYTES +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    this.request.getServiceName().length() +
                    Request.REQUEST_STRING_FIELD_LENGTH_BYTES +
                    this.request.getServiceInstanceIp().length() +
                    Request.REQUEST_INTEGER_FIELD_BYTES
            );
            byteBuffer.putInt(Request.REGISTER);
            byteBuffer.put(this.request.getId().getBytes());
            byteBuffer.putInt(this.request.getServiceName().length());
            byteBuffer.put(this.request.getServiceName().getBytes());
            byteBuffer.putInt(this.request.getServiceInstanceIp().length());
            byteBuffer.put(this.request.getServiceInstanceIp().getBytes());
            byteBuffer.putInt(this.request.getServiceInstancePort());
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

    public String getServiceInstanceIp() {
        return serviceInstanceIp;
    }

    public void setServiceInstanceIp(String serviceInstanceIp) {
        this.serviceInstanceIp = serviceInstanceIp;
    }

    public Integer getServiceInstancePort() {
        return serviceInstancePort;
    }

    public void setServiceInstancePort(Integer serviceInstancePort) {
        this.serviceInstancePort = serviceInstancePort;
    }

    /**
     * 反序列化获取请求对象
     * @param buffer
     * @return
     */
    public static RegisterRequest deserialize(ByteBuffer buffer) {
        // 解析请求id
        byte[] idBytes = new byte[Request.REQUEST_ID_BYTES];
        buffer.get(idBytes);
        String id = new String(idBytes);

        // 解析服务名称
        Integer serviceNameLength = buffer.getInt();
        byte[] serviceNameBytes = new byte[serviceNameLength];
        buffer.get(serviceNameBytes);
        String serviceName = new String(serviceNameBytes);

        // 解析服务实例ip地址
        Integer serviceInstanceIpLength = buffer.getInt();
        byte[] serviceInstanceIpBytes = new byte[serviceInstanceIpLength];
        buffer.get(serviceInstanceIpBytes);
        String serviceInstanceIp = new String(serviceInstanceIpBytes);

        // 解析服务实例端口号
        Integer serviceInstancePort = buffer.getInt();

        // 构建服务注册请求对象
        RegisterRequest request = new RegisterRequest();
        request.setId(id);
        request.setServiceName(serviceName);
        request.setServiceInstanceIp(serviceInstanceIp);
        request.setServiceInstancePort(serviceInstancePort);

        return request;
    }

    @Override
    public String toString() {
        return "RegisterRequest{" +
                "id='" + id + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", serviceInstanceIp='" + serviceInstanceIp + '\'' +
                ", serviceInstancePort=" + serviceInstancePort +
                '}';
    }
}
