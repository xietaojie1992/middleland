package com.middleland.examples.zookeeper.locks;

import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 共享锁: 全局同步分布式锁, 同一时间一个进程能获得同一把锁.
 *
 * Fully distributed locks that are globally synchronous, meaning at any snapshot in time no two clients think they hold the same lock.
 *
 * Note: unlike InterProcessMutex this lock is not reentrant.
 *
 */
@Slf4j
public class SharedLockTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorFramework curatorFramework;

    @Test
    public void testInterProcessMutex() throws Exception {
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 3; i++) {
                fixedThreadPool.submit(() -> {
                    while (true) {
                        try {
                            lock("/lock/sharedLock_1");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });
        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 3; i++) {
                fixedThreadPool.submit(() -> {
                    while (true) {
                        try {
                            lock("/lock/sharedLock_2");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });
        thread1.start();
        thread2.start();
        Thread.sleep(60 * 1000);
    }

    @Test
    public void testUnReentrant() throws Exception {
        InterProcessSemaphoreMutex mutex = new InterProcessSemaphoreMutex(curatorFramework, "/lock/sharedLock");
        mutex.acquire();
        Assert.assertFalse(mutex.acquire(5, TimeUnit.SECONDS));// 由于不可重入，获取锁肯定会超时，返回false
        mutex.release();
    }

    /**
     * 不可重入互斥锁
     * 注意：避免在递归场景下使用，会导致死锁
     *
     * @throws Exception
     */
    private void lock(String path) throws Exception {
        InterProcessSemaphoreMutex mutex = new InterProcessSemaphoreMutex(curatorFramework, path);
        try {
            mutex.acquire();
            log.info("Thread ID:{} acquire the lock:{}", Thread.currentThread().getId(), path);
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (mutex.isAcquiredInThisProcess()) {
                log.info("Thread ID:{} release the lock:{}", Thread.currentThread().getId(), path);
                mutex.release();
            } else {
                log.warn("Thread ID:{} is not required in this process", Thread.currentThread().getId());
            }
        }
    }

}
