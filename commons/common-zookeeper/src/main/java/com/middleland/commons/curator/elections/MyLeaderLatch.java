package com.middleland.commons.curator.elections;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatch.CloseMode;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * 1. 选出leader后，该实例会一直占有领导权，直到调用 close() 方法关闭当前主节点，然后其他 LeaderLatch 实例才会再次选举leader。
 * 2. close 之后不再参与选举
 * 这种策略适合主备应用，当主节点意外宕机之后，多个从节点会自动选举其中一个为新的主节点（Master节点）
 */
@Slf4j
public class MyLeaderLatch implements LeaderLatchListener, ConnectionStateListener, Closeable {

    private              CuratorFramework curatorFramework;
    private              LeaderLatch      leaderLatch;
    private              String           name;
    private final static String           LATCH_PATH = "/leader_latch";

    public MyLeaderLatch(CuratorFramework curatorFramework, String name) {
        this.curatorFramework = curatorFramework;
        this.name = name;

        this.leaderLatch = new LeaderLatch(curatorFramework, LATCH_PATH);
        this.leaderLatch.addListener(this);
    }

    public boolean hasLeadership() {
        return this.leaderLatch.hasLeadership();
    }

    public void start() throws Exception {
        leaderLatch.start();
    }

    @Override
    public void close() throws IOException {
        leaderLatch.close();
    }

    /**
     * Remove this instance from the leadership election. If this instance is the leader, leadership is released.
     * Noted: the only way to release leadership is by calling close(). All LeaderLatch instances must eventually be closed.
     */
    public void close(CloseMode closeMode) throws IOException {
        leaderLatch.close(closeMode);
    }

    @Override
    public void isLeader() {
        log.info("{} becomes leader", name);
    }

    @Override
    public void notLeader() {
        log.info("{} is not leader", name);
    }

    public Collection<Participant> getParticipants() throws Exception {
        return leaderLatch.getParticipants();
    }

    /**
     * 阻塞等待，直至成为 Leader，或 timeout/interrupted/close
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return leaderLatch.await(timeout, unit);
    }

    /**
     * 需要监听和 Zookeeper 的连接状态，如果连接异常，则认为自己已经不是 Leader
     *
     * @param client
     * @param newState
     */
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        log.info("{}: ZooKeeper Connection State Changed To: {}", name, newState.name());
        switch (newState) {
            case CONNECTED:
                break;
            case RECONNECTED:
                break;
            case READ_ONLY:
                break;
            case LOST:
            case SUSPENDED:
                log.info("{} is not leader due to connection problems", name);
                break;
            default:
        }
    }
}
