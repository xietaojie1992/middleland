package com.middleland.examples.zookeeper;

import com.middleland.commons.curator.CuratorInstance;
import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentTtlNode;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author xietaojie
 */
@Slf4j
public class ZookeeperTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorInstance curatorInstance;

    @Test
    public void test() {
        log.info("curator started? {}", curatorInstance.isStarted());

        CuratorFramework curatorFramework = curatorInstance.getCurator();
        try {
            curatorFramework.create().withTtl(10000).withMode(CreateMode.PERSISTENT_WITH_TTL).forPath("/path",
                    "172.16.5.70&100".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPersistentTtlNode() {
        CuratorFramework curatorFramework = curatorInstance.getCurator();
        PersistentTtlNode persistentTtlNode = new PersistentTtlNode(curatorFramework, "test_persistentTtlNode", 1000,
                "persistentTtlNode".getBytes());
    }
}
