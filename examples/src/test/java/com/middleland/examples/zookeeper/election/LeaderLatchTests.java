package com.middleland.examples.zookeeper.election;

import com.middleland.commons.curator.elections.MyLeaderLatch;
import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch.CloseMode;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LeaderLatchTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorFramework curatorFramework;

    @Test
    public void test() {
        MyLeaderLatch myLeaderLatch = new MyLeaderLatch(curatorFramework, "Test_1");

        try {
            myLeaderLatch.start();
            Thread.sleep(1000);
            Assert.assertTrue(myLeaderLatch.hasLeadership());

            myLeaderLatch.close();
            Assert.assertFalse(myLeaderLatch.hasLeadership());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAwait() {
        MyLeaderLatch myLeaderLatch1 = new MyLeaderLatch(curatorFramework, "Test_1");
        MyLeaderLatch myLeaderLatch2 = new MyLeaderLatch(curatorFramework, "Test_2");

        try {
            myLeaderLatch1.start();
            Thread.sleep(1000);
            myLeaderLatch2.start();
            Thread.sleep(1000);
            Assert.assertTrue(myLeaderLatch1.hasLeadership());
            Assert.assertFalse(myLeaderLatch2.hasLeadership());

            /**
             * 获取参与选举的 LeaderLatch ID
             */
            myLeaderLatch1.getParticipants().forEach(l -> log.info(l.getId()));

            Assert.assertFalse(myLeaderLatch2.await(5, TimeUnit.SECONDS));

            new Thread(() -> {
                try {
                    Thread.sleep(1000);

                    myLeaderLatch1.close(CloseMode.NOTIFY_LEADER);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();

            Assert.assertTrue(myLeaderLatch2.await(5, TimeUnit.SECONDS));

            Assert.assertTrue(myLeaderLatch2.hasLeadership());
            Assert.assertFalse(myLeaderLatch1.hasLeadership());

            myLeaderLatch2.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
