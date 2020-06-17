package com.middleland.locks.zookeeper;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xietaojie
 */
@Slf4j
public class ServiceLocksManager implements LockChangeListener, TreeCacheListener {

    private final static String  LOCK_PREFIX     = "/service_locks/";
    private final static Pattern PATTERN_service = Pattern.compile("/(service:\\d+)/.*-latch-(\\d+)");

    private final Map<String, ServiceLock>                            serviceLockMap     = new ConcurrentHashMap<>();
    private final List<LockChangeListener>                            listeners          = new ArrayList<>();
    private final Random                                              random             = new Random();
    private final ConcurrentHashMap<String, TreeSet<ParticipantInfo>> serviceMap         = new ConcurrentHashMap<>();
    private       Collection<NodeReadWriteLock>                       nodeReadWriteLocks = Collections.synchronizedCollection(
            new HashSet<>());

    /**
     * EdgeSouth 服务 ID
     */
    private final String           participant;
    private final CuratorFramework curatorFramework;
    private       TreeCache        treeCache;

    public ServiceLocksManager(CuratorFramework curatorFramework, String participant) {
        log.info("ServiceLocksManager construct, participant={}", participant);
        this.curatorFramework = curatorFramework;
        this.participant = participant;
        try {
            this.treeCache = TreeCache.newBuilder(curatorFramework, LOCK_PREFIX.substring(0, LOCK_PREFIX.length() - 1)).setCacheData(true)
                    .setCreateParentNodes(true).build();
            treeCache.start();
            treeCache.getListenable().addListener(this);
        } catch (Exception e) {
            log.error("TreeCache Error.", e);
            e.printStackTrace();
        }
    }

    public void addListener(LockChangeListener listener) {
        listeners.add(listener);
    }

    public void removeListener(LockChangeListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void onLockChanged(boolean hasLock, String nodeId, String reason) {
        listeners.forEach(l -> l.onLockChanged(hasLock, nodeId, reason));
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("{}", event.toString());
        }
        switch (event.getType()) {
            case INITIALIZED:
                break;
            case NODE_ADDED: {
                Matcher matcher = PATTERN_service.matcher(event.getData().getPath());
                if (matcher.find()) {
                    String nodeId = matcher.group(1);
                    Integer seqId = Integer.valueOf(matcher.group(2));
                    String participant = new String(event.getData().getData());
                    log.info("nodeAdded, nodeId={}, participant={}", nodeId, participant);
                    if (StringUtils.isNotBlank(participant)) {
                        TreeSet<ParticipantInfo> servers = serviceMap.get(nodeId);
                        if (null == servers) {
                            servers = new TreeSet<>(new ParticipantComparator());
                            serviceMap.put(nodeId, servers);
                        }
                        servers.add(new ParticipantInfo(participant, seqId));
                    }
                }
                break;
            }
            case NODE_REMOVED: {
                Matcher matcher = PATTERN_service.matcher(event.getData().getPath());
                if (matcher.find()) {
                    String nodeId = matcher.group(1);
                    Integer seqId = Integer.valueOf(matcher.group(2));
                    String participant = new String(event.getData().getData());
                    log.info("nodeRemoved, nodeId={}, participant={}", nodeId, participant);
                    Set<ParticipantInfo> servers = serviceMap.get(nodeId);
                    if (StringUtils.isNotBlank(participant) && null != servers && !servers.isEmpty()) {
                        servers.remove(new ParticipantInfo(participant, seqId));
                    } else {
                        serviceMap.remove(nodeId);
                    }
                }
                break;
            }
            case NODE_UPDATED: {
                Matcher matcher = PATTERN_service.matcher(event.getData().getPath());
                if (matcher.find()) {
                    String nodeId = matcher.group(1);
                    String serverId = new String(event.getData().getData());
                    log.info("nodeUpdated, nodeId={}, participant={}", nodeId, serverId);
                }
                break;
            }
            case CONNECTION_LOST:
                log.info("CONNECTION_LOST");
                break;
            case CONNECTION_SUSPENDED:
                log.info("CONNECTION_SUSPENDED");
                break;
            case CONNECTION_RECONNECTED:
                log.info("CONNECTION_RECONNECTED");
                break;
            default:
        }
    }

    /**
     * 获取所有的设备及其抢锁者的信息
     *
     * @return
     */
    //public Set<Map.Entry<String, Set<String>>> graph() {
    //    return serviceMap.entrySet();
    //}

    /**
     *
     * @param nodeId
     * @return
     */
    public List<String> getParticipants(String nodeId) {
        Lock readLock = getNodeReadWriteLockObj(nodeId).getReadLock();
        readLock.lock();
        try {
            Set<ParticipantInfo> servers = serviceMap.get(nodeId);
            if (null == servers) {
                return Collections.emptyList();
            }
            List<String> participants = new ArrayList<>(servers.size());
            servers.forEach(p -> participants.add(p.getParticipant()));
            return participants;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取当前的竞锁者状态
     *
     * @param nodeId
     * @return
     */
    public Map<String, Boolean> getParticipantsRole(String nodeId) {
        Lock readLock = getNodeReadWriteLockObj(nodeId).getReadLock();
        readLock.lock();
        try {
            ServiceLock serviceLock = serviceLockMap.get(nodeId);
            if (serviceLock != null) {
                return serviceLock.getParticipants();
            } else {
                Set<ParticipantInfo> servers = serviceMap.get(nodeId);
                if (null != servers) {
                    Map<String, Boolean> map = new HashMap<>(servers.size());
                    AtomicReference<Boolean> masterFlag = new AtomicReference<>(true);
                    servers.forEach(participantInfo -> {
                        if (masterFlag.compareAndSet(true, false)) {
                            map.put(participantInfo.getParticipant(), true);
                        } else {
                            map.put(participantInfo.getParticipant(), false);
                        }
                    });
                    return map;
                }
            }
        } finally {
            readLock.unlock();
        }
        return Collections.emptyMap();
    }

    public String getRandomParticipant(String nodeId) {
        Set<ParticipantInfo> servers = serviceMap.get(nodeId);
        if (null == servers) {
            return null;
        }
        return Lists.newArrayList(servers).get(random.nextInt(servers.size())).getParticipant();
    }

    public String getLockOwner(String nodeId) {
        Lock readLock = getNodeReadWriteLockObj(nodeId).getReadLock();
        readLock.lock();
        try {
            ServiceLock serviceLock = serviceLockMap.get(nodeId);
            if (serviceLock != null) {
                return serviceLock.getLeaderId();
            }
            Set<ParticipantInfo> servers = serviceMap.get(nodeId);
            return servers == null ? null : servers.stream().findFirst().get().getParticipant();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取当前实例拥有锁的所有设备
     *
     * @return
     */
    public List<String> getLockedNodes() {
        List<String> lockedNodes = new ArrayList<>();
        for (Map.Entry<String, ServiceLock> entry : serviceLockMap.entrySet()) {
            if (entry.getValue().hasLeadership()) {
                lockedNodes.add(entry.getKey());
            }
        }
        return lockedNodes;
    }

    public void tryLock(String nodeId) {
        Lock writeLock = getNodeReadWriteLockObj(nodeId).getWriteLock();
        writeLock.lock();
        try {
            ServiceLock serviceLock = serviceLockMap.get(nodeId);
            if (serviceLock == null) {
                serviceLock = new ServiceLock(curatorFramework, participant, LOCK_PREFIX, nodeId, this::onLockChanged);
            }
            if (serviceLockMap.putIfAbsent(nodeId, serviceLock) == null) {
                serviceLock.tryLock();
                log.info("tryLock for {}", nodeId);
            } else {
                log.warn("ServiceLock-{} already existed.", nodeId);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean hasLock(String nodeId) {
        Lock readLock = getNodeReadWriteLockObj(nodeId).getReadLock();
        readLock.lock();
        try {
            log.debug("node {} hasLock readLock", nodeId);
            ServiceLock serviceLock = serviceLockMap.get(nodeId);
            if (serviceLock != null) {
                return this.serviceLockMap.get(nodeId).hasLeadership();
            }
            return false;
        } finally {
            readLock.unlock();
            log.debug("node {} hasLock unlock", nodeId);
        }

    }

    public boolean contains(String nodeId) {
        Lock readLock = getNodeReadWriteLockObj(nodeId).getReadLock();
        readLock.lock();
        try {
            return serviceLockMap.containsKey(nodeId);
        } finally {
            readLock.unlock();
        }

    }

    /**
     * 退出抢锁
     *
     * @param nodeId
     */
    public void remove(String nodeId) {
        Lock writeLock = getNodeReadWriteLockObj(nodeId).getWriteLock();
        writeLock.lock();
        try {
            ServiceLock serviceLock = serviceLockMap.get(nodeId);
            if (serviceLock != null) {
                try {
                    serviceLock.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    serviceLockMap.remove(nodeId);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 退出抢锁，并返回当前的竞锁者
     *
     * @param nodeId
     * @return
     */
    public Map<String, Boolean> removeAndGetRestParticipants(String nodeId) {
        Lock writeLock = getNodeReadWriteLockObj(nodeId).getWriteLock();
        writeLock.lock();
        try {
            log.debug("node {} removeAndGetRestParticipants writeLock", nodeId);
            ServiceLock serviceLock = serviceLockMap.get(nodeId);
            if (serviceLock != null) {
                try {
                    serviceLock.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    serviceLockMap.remove(nodeId);
                }
                return serviceLock.getParticipants();
            } else {
                Map<String, Boolean> m = getParticipantsRole(nodeId);
                return m == null ? Collections.emptyMap() : m;
            }
        } finally {
            writeLock.unlock();
            log.debug("node {} removeAndGetRestParticipants unlock", nodeId);
        }
    }

    private synchronized NodeReadWriteLock getNodeReadWriteLockObj(String nodeId) {
        Optional<NodeReadWriteLock> lock = nodeReadWriteLocks.stream().filter(l -> l.getNodeId().equals(nodeId)).findFirst();
        if (lock.isPresent()) {
            return lock.get();
        } else {
            NodeReadWriteLock nodeLock = new NodeReadWriteLock(nodeId);
            nodeReadWriteLocks.add(nodeLock);
            return nodeLock;
        }
    }

    static class NodeReadWriteLock {
        private String                 nodeId;
        private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

        public NodeReadWriteLock(String nodeId) {
            this.nodeId = nodeId;
        }

        public String getNodeId() {
            return nodeId;
        }

        public Lock getWriteLock() {
            return readWriteLock.writeLock();
        }

        public Lock getReadLock() {
            return readWriteLock.readLock();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NodeReadWriteLock nodeLock = (NodeReadWriteLock) o;
            return Objects.equals(nodeId, nodeLock.nodeId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId);
        }
    }

}
