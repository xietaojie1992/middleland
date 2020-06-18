package com.middleland.examples.zookeeper.locks;

import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * 共享信号量: 在分布式系统中的各个JVM使用同一个zk lock path，该path将跟一个给定数量的租约(lease)相关联，然后各个应用根据请求顺序获得对应的lease，
 * 相对来说，这是最公平的锁服务使用方式。
 */
@Slf4j
public class SharedSemaphoreTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorFramework curatorFramework;

    @Test
    public void test() {
        final InterProcessSemaphoreV2 semaphoreV2 = new InterProcessSemaphoreV2(curatorFramework, "/lock/sharedSemaphore", 3);
        //final InterProcessSemaphoreV2 semaphoreV2 = new InterProcessSemaphoreV2(curatorFramework, "/lock/sharedSemaphore",
        //        new SharedCount(curatorFramework, "/lock/semaphore", 3));
        List<Thread> jobs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            jobs.add(new Thread(() -> sharedSemaphore(semaphoreV2)));
        }

        jobs.forEach(j -> j.start());
        jobs.forEach(j -> {
            try {
                j.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

    }

    /**
     * 不同 Thread 进行 PV 操作
     */
    @Test
    public void test2() {
        final InterProcessSemaphoreV2 semaphoreV2 = new InterProcessSemaphoreV2(curatorFramework, "/lock/sharedSemaphore", 3);
        final Lease[] lease = {null};
        Thread pThread = new Thread(() -> {
            try {
                lease[0] = semaphoreV2.acquire();
                log.info("Thread ID:{} acquire the lock", Thread.currentThread().getId());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread vThread = new Thread(() -> {
            if (lease.length > 0 && lease[0] != null) {
                semaphoreV2.returnLease(lease[0]);
                log.info("Thread ID:{} release the lock", Thread.currentThread().getId());
            }
        });

        try {
            pThread.start();
            pThread.join();
            vThread.start();
            pThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 共享信号量
     *
     * @param semaphoreV2
     */
    private void sharedSemaphore(InterProcessSemaphoreV2 semaphoreV2) {
        Lease lease = null;
        try {
            lease = semaphoreV2.acquire();
            if (lease != null) {
                log.info("Thread ID:{} acquire the lock", Thread.currentThread().getId());
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (lease != null) {
                    semaphoreV2.returnLease(lease);
                    log.info("Thread ID:{} release the lock", Thread.currentThread().getId());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

}
