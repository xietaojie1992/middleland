package com.middleland.examples.zookeeper.locks;

import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fully distributed locks that are globally synchronous, meaning at any snapshot in time no two clients think they hold the same lock.
 *
 */
@Slf4j
public class SharedReentrantLockTests extends ExamplesApplicationTests {

    @Autowired
    private CuratorFramework curatorFramework;

    @Test
    public void testInterProcessMutex() throws Exception {
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                fixedThreadPool.submit(() -> {
                    while (true) {
                        try {
                            lock("/lock/sharedReentantLock_1");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });
        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                fixedThreadPool.submit(() -> {
                    while (true) {
                        try {
                            lock("/lock/sharedReentantLock_2");
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
    public void testInterProcessMutexReentant() throws Exception {
        final InterProcessMutex mutex = new InterProcessMutex(curatorFramework, "/lock/sharedReentantLock");
        log.info("Current Thread ID = " + Thread.currentThread().getId());
        AtomicInteger atomicInteger = new AtomicInteger(0);

        mutex.acquire();
        log.info("Thread ID:{} acquire the lock, count:{}", Thread.currentThread().getId(), atomicInteger.incrementAndGet());
        mutex.acquire();
        log.info("Thread ID:{} acquire the lock, count:{}", Thread.currentThread().getId(), atomicInteger.incrementAndGet());
        mutex.acquire();
        log.info("Thread ID:{} acquire the lock, count:{}", Thread.currentThread().getId(), atomicInteger.incrementAndGet());

        mutex.release();
        log.info("Thread ID:{} release the lock, count:{}", Thread.currentThread().getId(), atomicInteger.decrementAndGet());
        mutex.release();
        log.info("Thread ID:{} release the lock, count:{}", Thread.currentThread().getId(), atomicInteger.decrementAndGet());
        mutex.release();
        log.info("Thread ID:{} release the lock, count:{}", Thread.currentThread().getId(), atomicInteger.decrementAndGet());
    }

    /**
     * 公平可重入互斥锁
     *
     * acquire()方法，在给定的路径下创建临时有序的节点。然后它会和父节点下面的其他节点比较时序。
     * 如果客户端创建的临时时序节点的数字后缀最小的话，则获得该锁，函数成功返回。
     * 如果没有获得到，即，创建的临时节点数字后缀不是最小的，则启动一个watch监听上一个（排在前面一个的节点）。
     * 主线程使用object.wait()进行等待，等待watch触发的线程notifyAll()，一旦上一个节点有事件产生马上再次出发时序最小节点的判断。
     *
     * release()方法就是释放锁，内部实现就是删除创建的EPHEMERAL_SEQUENTIAL节点。
     *
     * @throws Exception
     */
    private void lock(String path) throws Exception {
        final InterProcessMutex mutex = new InterProcessMutex(curatorFramework, path);
        try {
            mutex.acquire();
            log.info("Thread ID:{} acquire the lock:{}", Thread.currentThread().getId(), path);
            Thread.sleep(30000);
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
