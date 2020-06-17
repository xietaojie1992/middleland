package com.middleland.locks.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatch.CloseMode;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 获取服务锁
 *
 * 场景：N 台控制中心，同时控制着 M 个服务，每个服务都会和控制中心进行交互，但只需要其中一台控制处理单个服务的所有信息
 *
 * @author xietaojie
 */
@Slf4j
public class ServiceLock implements LeaderLatchListener, ConnectionStateListener {

    private final LockChangeListener lockChangeListener;
    private final String             serviceId;
    private final String             participant;
    private       LeaderLatch        leaderLatch;
    private       boolean            hasLock = false;

    public ServiceLock(CuratorFramework curatorFramework, String participant, String lockPrefix, String serviceId,
                       LockChangeListener lockChangeListener) {
        this.serviceId = serviceId;
        this.participant = participant;
        this.lockChangeListener = lockChangeListener;

        this.leaderLatch = new LeaderLatch(curatorFramework, lockPrefix + serviceId, participant);
        this.leaderLatch.addListener(this);
    }

    public Map<String, Boolean> getParticipants() {
        try {
            Collection<Participant> collection = leaderLatch.getParticipants();
            Map<String, Boolean> result = new HashMap<>(collection.size());
            leaderLatch.getParticipants().forEach(participant -> {
                result.put(participant.getId(), participant.isLeader());
            });
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();
    }

    public String getLeaderId() {
        try {
            return leaderLatch.getLeader().getId();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void tryLock() {
        try {
            this.leaderLatch.start();
        } catch (Exception e) {
            log.error("ServiceLock[{}] started failed, {}", serviceId, e);
        }
    }

    public boolean hasLeadership() {
        return this.leaderLatch.hasLeadership();
    }

    public void close() throws IOException {
        leaderLatch.close(CloseMode.NOTIFY_LEADER);
    }

    @Override
    public void isLeader() {
        log.info("participant-{} has Lock of {}", participant, serviceId);
        this.hasLock = true;
        lockChangeListener.onLockChanged(true, serviceId, "GET_LOCK");
    }

    @Override
    public void notLeader() {
        log.info("participant-{} loss Lock of {}", participant, serviceId);
        this.hasLock = false;
        lockChangeListener.onLockChanged(false, serviceId, "LOSS_LOCK");
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        log.info("{}: ZooKeeper Connection State Changed To: {}", serviceId, connectionState.name());
        switch (connectionState) {
            case CONNECTED:
            case RECONNECTED:
            case READ_ONLY:
                break;
            case SUSPENDED:
                // 对网络不稳定进行容错，不处理 SUSPENDED
                break;
            case LOST:
                log.info("{} is not leader due to connection problems", serviceId);
                if (this.hasLock) {
                    this.hasLock = false;
                    lockChangeListener.onLockChanged(false, serviceId, "ConnectionState change to " + connectionState.name());
                }
                break;
            default:
        }
    }
}
