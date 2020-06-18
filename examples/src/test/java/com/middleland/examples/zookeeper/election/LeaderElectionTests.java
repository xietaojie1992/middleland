package com.middleland.examples.zookeeper.election;

import com.middleland.commons.curator.elections.MyLeaderSelector;
import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class LeaderElectionTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorFramework curatorFramework;

    @Test
    public void test() {
        MyLeaderSelector myLeaderSelector = new MyLeaderSelector(curatorFramework, "1");
        MyLeaderSelector myLeaderSelector2 = new MyLeaderSelector(curatorFramework, "2");
        try {
            Thread.sleep(1000 * 60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
