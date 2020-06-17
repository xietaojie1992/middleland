package com.middleland.examples.zookeeper;

import com.middleland.commons.curator.CuratorConfig;
import com.middleland.commons.curator.CuratorInstance;
import com.middleland.commons.curator.CuratorInstanceImpl;
import com.middleland.examples.ExamplesApplicationTests;
import com.middleland.locks.zookeeper.ServiceLocksManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Random;

/**
 * @author xietaojie
 */
@Slf4j
public class ServiceLocksManagerTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorInstance curatorInstance;

    @Autowired
    private CuratorConfig curatorConfig;

    @Test
    public void testServiceLock() throws Exception {
        CuratorInstance curatorInstance1 = new CuratorInstanceImpl(curatorConfig);

        ServiceLocksManager instance = new ServiceLocksManager(curatorInstance.getCurator(), "1-egwge&22323");
        ServiceLocksManager instance2 = new ServiceLocksManager(curatorInstance.getCurator(), "2-egwge&22323");
        ServiceLocksManager instance3 = new ServiceLocksManager(curatorInstance1.getCurator(), "3-egwge&22323");
        String service = "service:123";
        instance.tryLock(service);
        instance2.tryLock(service);
        Assert.assertTrue(instance.contains(service));
        Thread.sleep(500);
        log.info("Leader ID: {}", instance.getLockOwner(service));
        Assert.assertTrue(instance.hasLock(service) || instance2.hasLock(service));

        log.info("instance1 lockedNodes: {}", instance.getLockedNodes());
        log.info("instance2 lockedNodes: {}", instance2.getLockedNodes());
        log.info("instance2 lockedNodes: {}", instance3.getLockedNodes());
        log.info("serviceId={}, leader={}, participants={}", service, instance.getLockOwner(service),
                instance.getParticipantsRole(service));
        log.info("serviceId={}, leader={}, participants={}", service, instance2.getLockOwner(service),
                instance2.getParticipantsRole(service));
        log.info("serviceId={}, leader={}, participants={}", service, instance3.getLockOwner(service),
                instance3.getParticipantsRole(service));

        instance.remove(service);
        log.info("serviceId={}, leader={}, participants={}", service, instance.getLockOwner(service),
                instance.getParticipantsRole(service));
        Assert.assertFalse(instance.contains(service));

        instance.tryLock(service);
        instance3.tryLock(service);
        System.in.read();
    }

    @Test
    public void serviceLockPressureTest() throws InterruptedException, IOException {

        pressureTryLock("1");
        pressureTryLock("2");
        pressureTryLock("3");

        System.in.read();
    }

    private void pressureTryLock(String participant) throws InterruptedException {

        ServiceLocksManager locksManager = new ServiceLocksManager(curatorInstance.getCurator(), participant);
        int num = 100;
        for (int i = 1; i <= num; i++) {
            String lockName = "service:" + i;
            locksManager.tryLock(lockName);
            Assert.assertTrue(locksManager.contains(lockName));
        }

        Random random = new Random();
        new Thread(() -> {
            while (true) {
                try {
                    String dpid = "service:" + (random.nextInt(num - 1) + 1);

                    log.info("serviceId={}, participants={}", dpid, locksManager.getParticipants(dpid));

                    locksManager.remove(dpid);
                    Thread.sleep(1000);
                    locksManager.tryLock(dpid);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
