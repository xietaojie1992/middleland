package com.middleland.commons.curator.elections;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.io.Closeable;
import java.io.IOException;

/**
 * 1. 每个实例都能公平获取领导权，而且当获取领导权的实例在释放领导权之后，该实例还有机会再次获取领导权；
 * 2. 选举出来的 Leader 不会一直占有领导权，当 takeLeadership(CuratorFramework client) 方法执行结束之后会自动释放领导权。
 */
@Slf4j
public class MyLeaderSelector extends LeaderSelectorListenerAdapter implements Closeable {

    private CuratorFramework curatorFramework;
    private LeaderSelector   leaderSelector;
    private String           id;

    private static final String PATH = "/leader_selector";

    public MyLeaderSelector(CuratorFramework curatorFramework, String id) {
        this.curatorFramework = curatorFramework;
        this.id = id;

        this.leaderSelector = new LeaderSelector(curatorFramework, PATH, this);
        this.leaderSelector.setId(id);
        this.leaderSelector.autoRequeue();// 失去 Leadership 之后，重新自动排队进行选举
        this.leaderSelector.start();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        log.info("{} take leadership", id);
        Thread.sleep(1000 * 60);
        log.info("{} process finished...", id);
    }

    public boolean hasLeadership() {
        return this.leaderSelector.hasLeadership();
    }

    public void start() throws IOException {
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

}
