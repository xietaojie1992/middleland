package com.middleland.examples.zookeeper.locks;

import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 */
@Slf4j
public class SharedReentrantReadWriteLockTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorFramework curatorFramework;

    @Test
    public void testReadLock() {
        log.info("Test Read Lock");
        InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(curatorFramework, "/lock/SharedReentrantReadWriteLock");
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(executor.submit(new ReadWriteLockCallable(readWriteLock.readLock(), "read")));
        }
        futures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testWriteLock() {
        log.info("Test Write Lock");
        InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(curatorFramework, "/lock/SharedReentrantReadWriteLock");
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future> futures2 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures2.add(executor.submit(new ReadWriteLockCallable(readWriteLock.writeLock(), "write")));
        }
        futures2.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    class ReadWriteLockCallable implements Callable<Void> {

        private InterProcessMutex interProcessMutex;
        private String            lockType;

        public ReadWriteLockCallable(InterProcessMutex mutex, String lockType) {
            this.interProcessMutex = mutex;
            this.lockType = lockType;
        }

        @Override
        public Void call() throws Exception {
            try {
                interProcessMutex.acquire();
                log.info("{} acquire {} lock", Thread.currentThread().getId(), lockType);
                Thread.sleep(200);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    interProcessMutex.release();
                    log.info("{} release {} lock", Thread.currentThread().getId(), lockType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }
}
