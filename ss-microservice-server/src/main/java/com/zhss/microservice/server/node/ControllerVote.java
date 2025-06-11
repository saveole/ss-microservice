package com.zhss.microservice.server.node;

import com.zhss.microservice.server.constant.MessageType;

import java.nio.ByteBuffer;

/**
 * 选票
 */
public class ControllerVote {

    /**
     * 投票节点id
     */
    private Integer voterNodeId;
    /**
     * 投票给的那个controller节点id
     */
    private Integer controllerNodeId;
    /**
     * 投票的轮次
     */
    private Integer voteRound;

    public ControllerVote(Integer voterNodeId, Integer controllerNodeId, Integer voteRound) {
        this.voterNodeId = voterNodeId;
        this.controllerNodeId = controllerNodeId;
        this.voteRound = voteRound;
    }

    public ControllerVote(ByteBuffer message) {
        this.voterNodeId = message.getInt();
        this.controllerNodeId = message.getInt();
        this.voteRound = message.getInt();
    }

    public Integer getVoterNodeId() {
        return voterNodeId;
    }

    public void setVoterNodeId(Integer voterNodeId) {
        this.voterNodeId = voterNodeId;
    }

    public Integer getControllerNodeId() {
        return controllerNodeId;
    }

    public void setControllerNodeId(Integer controllerNodeId) {
        this.controllerNodeId = controllerNodeId;
    }

    public Integer getVoteRound() {
        return voteRound;
    }

    public void setVoteRound(Integer voteRound) {
        this.voteRound = voteRound;
    }

    /**
     * 获取选票的请求ByteBuffer数据
     * @return
     */
    public ByteBuffer getMessageByteBuffer() {
        byte[] bytes = new byte[16];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.clear();

        byteBuffer.putInt(MessageType.VOTE);
        byteBuffer.putInt(voterNodeId);
        byteBuffer.putInt(controllerNodeId);
        byteBuffer.putInt(voteRound);

        return byteBuffer;
    }

    @Override
    public String toString() {
        return "Vote{" +
                "voterNodeId=" + voterNodeId +
                ", controllerNodeId=" + controllerNodeId +
                ", voteRound=" + voteRound +
                '}';
    }

}
