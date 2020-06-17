package com.middleland.locks.zookeeper;

import com.middleland.commons.common.base.NoRollbackException;
import com.middleland.commons.common.base.utils.KeyUtils;
import com.middleland.commons.curator.CuratorInstance;
import com.middleland.locks.base.LockExecutor;
import com.middleland.locks.base.LockType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author xietaojie
 */
@Slf4j
public class ZookeeperLockExecutorImpl implements LockExecutor<InterProcessMutex> {

    private CuratorInstance curatorInstance;
    private String          prefix;

    private volatile Map<String, InterProcessMutex>         lockMap          = new ConcurrentHashMap<>();
    private volatile Map<String, InterProcessReadWriteLock> readWriteLockMap = new ConcurrentHashMap<>();
    private          boolean                                lockCached       = true;

    public ZookeeperLockExecutorImpl(CuratorInstance curatorInstance, String prefix) {
        this.curatorInstance = curatorInstance;
        this.prefix = prefix;
    }

    @PreDestroy
    public void destroy() {
        try {
            curatorInstance.close();
        } catch (Exception e) {
            throw new NoRollbackException("Close Curator failed", e);
        }
    }

    @Override
    public InterProcessMutex tryLock(LockType lockType, String name, String key, long leaseTime, long waitTime, boolean async, boolean fair)
            throws Exception {
        if (StringUtils.isEmpty(name)) {
            throw new NoRollbackException("Name is null or empty");
        }

        if (StringUtils.isEmpty(key)) {
            throw new NoRollbackException("Key is null or empty");
        }

        String compositeKey = KeyUtils.getCompositeKey(prefix, name, key);

        return tryLock(lockType, compositeKey, leaseTime, waitTime, async, fair);
    }

    @Override
    public InterProcessMutex tryLock(LockType lockType, String compositeKey, long leaseTime, long waitTime, boolean async, boolean fair)
            throws Exception {
        if (StringUtils.isEmpty(compositeKey)) {
            throw new NoRollbackException("Composite key is null or empty");
        }

        if (fair) {
            throw new NoRollbackException("Fair lock of Zookeeper isn't support for " + lockType);
        }

        if (async) {
            throw new NoRollbackException("Async lock of Zookeeper isn't support for " + lockType);
        }
        return tryLock(lockType, compositeKey, waitTime);
    }

    public InterProcessMutex tryLock(LockType lockType, String compositeKey, long waitTime) throws Exception {

        curatorInstance.validateStartedStatus();

        InterProcessMutex interProcessMutex = getLock(lockType, compositeKey);
        boolean acquired = interProcessMutex.acquire(waitTime, TimeUnit.MILLISECONDS);

        return acquired ? interProcessMutex : null;
    }

    @Override
    public void unlock(InterProcessMutex interProcessMutex) throws Exception {
        if (curatorInstance.isStarted()) {
            if (interProcessMutex != null && interProcessMutex.isAcquiredInThisProcess()) {
                interProcessMutex.release();
            }
        }
    }

    private InterProcessMutex getLock(LockType lockType, String key) {
        if (lockCached) {
            return getCachedLock(lockType, key);
        } else {
            return getNewLock(lockType, key);
        }
    }

    private InterProcessMutex getNewLock(LockType lockType, String key) {
        String path = curatorInstance.getPath(prefix, key);
        CuratorFramework curator = curatorInstance.getCurator();
        switch (lockType) {
            case LOCK:
                return new InterProcessMutex(curator, path);
            case READ_LOCK:
                return getCachedReadWriteLock(key).readLock();
            // return new InterProcessReadWriteLock(curator, path).readLock();
            case WRITE_LOCK:
                return getCachedReadWriteLock(key).writeLock();
            // return new InterProcessReadWriteLock(curator, path).writeLock();
            default:
        }

        throw new NoRollbackException("Invalid Zookeeper lock type for " + lockType);
    }

    private InterProcessMutex getCachedLock(LockType lockType, String key) {
        String path = curatorInstance.getPath(prefix, key);
        String newKey = path + "-" + lockType;

        InterProcessMutex lock = lockMap.get(newKey);
        if (lock == null) {
            InterProcessMutex newLock = getNewLock(lockType, key);
            lock = lockMap.putIfAbsent(newKey, newLock);
            if (lock == null) {
                lock = newLock;
            }
        }

        return lock;
    }

    private InterProcessReadWriteLock getCachedReadWriteLock(String key) {
        String path = curatorInstance.getPath(prefix, key);
        String newKey = path;

        InterProcessReadWriteLock readWriteLock = readWriteLockMap.get(newKey);
        if (readWriteLock == null) {
            CuratorFramework curator = curatorInstance.getCurator();
            InterProcessReadWriteLock newReadWriteLock = new InterProcessReadWriteLock(curator, path);
            readWriteLock = readWriteLockMap.putIfAbsent(newKey, newReadWriteLock);
            if (readWriteLock == null) {
                readWriteLock = newReadWriteLock;
            }
        }

        return readWriteLock;
    }
}