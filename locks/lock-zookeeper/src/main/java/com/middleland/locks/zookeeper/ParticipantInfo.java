package com.middleland.locks.zookeeper;

import com.google.common.base.Objects;

/**
 * @author xietaojie
 */
public class ParticipantInfo {

    private String  participant;
    private Integer seqId;

    public ParticipantInfo(String participant, Integer seqId) {
        this.participant = participant;
        this.seqId = seqId;
    }

    @Override
    public String toString() {
        return "ParticipantInfo{" + "participant='" + participant + '\'' + ", seqId=" + seqId + '}';
    }

    public String getParticipant() {
        return participant;
    }

    public void setParticipant(String participant) {
        this.participant = participant;
    }

    public Integer getSeqId() {
        return seqId;
    }

    public void setSeqId(Integer seqId) {
        this.seqId = seqId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParticipantInfo that = (ParticipantInfo) o;
        return Objects.equal(participant, that.participant) && Objects.equal(seqId, that.seqId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(participant, seqId);
    }
}
