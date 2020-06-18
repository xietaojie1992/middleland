package com.middleland.examples.zookeeper.barriers;

import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author xietaojie1992
 */
@Slf4j
public class BarrierTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorFramework curatorFramework;

    private final String barrierPath = "/barrier";

    @Test
    public void test() {
        DistributedBarrier distributedBarrier = new DistributedBarrier(curatorFramework, barrierPath);
        //distributedBarrier.
    }

}
