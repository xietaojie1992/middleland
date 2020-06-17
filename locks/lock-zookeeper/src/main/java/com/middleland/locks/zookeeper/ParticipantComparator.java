package com.middleland.locks.zookeeper;

import java.util.Comparator;

/**
 * 用于 ParticipantInfo 排序，第一位说明获取了锁
 *
 * @author xietaojie
 */
public class ParticipantComparator implements Comparator<ParticipantInfo> {

    @Override
    public int compare(ParticipantInfo o1, ParticipantInfo o2) {
        if (o1 == null || o1.getSeqId() == null) {
            return 1;
        } else if (o2 == null || o2.getSeqId() == null) {
            return -1;
        }
        return o1.getSeqId() - o2.getSeqId();
    }
}
