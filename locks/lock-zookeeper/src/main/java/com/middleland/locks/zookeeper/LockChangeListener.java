package com.middleland.locks.zookeeper;

/**
 * @author xietaojie
 */
public interface LockChangeListener {

    /**
     * 监听当前实例持有锁的状态变化
     *
     * @param hasLock
     * @param nodeId
     * @param reason
     */
    void onLockChanged(boolean hasLock, String nodeId, String reason);

}
