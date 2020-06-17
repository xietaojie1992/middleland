package com.middleland.commons.curator;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.PathUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author xietaojie
 */
@Slf4j
public class CuratorInstanceImpl implements CuratorInstance {

    private       CuratorFramework curatorFramework;
    private final CuratorConfig    curatorConfig;

    public CuratorInstanceImpl(CuratorConfig curatorConfig) throws Exception {
        this.curatorConfig = curatorConfig;
        initialize();
    }

    /**
     * 创建Curator，并初始化根节点
     */
    private void initialize() throws Exception {
        log.info("CuratorInstance initializing, CuratorConfig : {}", curatorConfig.toString());
        if (StringUtils.isEmpty(curatorConfig.getConnectString())) {
            throw new CuratorException("connectString can't be null or empty");
        }

        RetryPolicy retryPolicy;
        RetryTypeEnum retryTypeEnum = RetryTypeEnum.fromString(curatorConfig.getRetryType());
        switch (retryTypeEnum) {
            case EXPONENTIAL_BACKOFF_RETRY: {
                retryPolicy = RetryPolicyCreator.createExponentialBackoffRetry(
                        curatorConfig.getExponentialBackoffRetry().getBaseSleepTimeMs(),
                        curatorConfig.getExponentialBackoffRetry().getMaxRetries());
                break;
            }
            case BOUNDED_EXPONENTIAL_BACKOFF_RETRY: {
                retryPolicy = RetryPolicyCreator.createBoundedExponentialBackoffRetry(
                        curatorConfig.getBoundedExponentialBackoffRetry().getBaseSleepTimeMs(),
                        curatorConfig.getBoundedExponentialBackoffRetry().getMaxSleepTimeMs(),
                        curatorConfig.getBoundedExponentialBackoffRetry().getMaxRetries());
                break;
            }
            case RETRY_NTIMES: {
                retryPolicy = RetryPolicyCreator.createRetryNTimes(curatorConfig.getRetryNTimes().getCount(),
                        curatorConfig.getRetryNTimes().getSleepMsBetweenRetries());
                break;
            }
            case RETRY_FOREVER: {
                retryPolicy = RetryPolicyCreator.createRetryForever(curatorConfig.getRetryForever().getRetryIntervalMs());
                break;
            }
            case RETRY_UNTIL_ELAPSED: {
                retryPolicy = RetryPolicyCreator.createRetryUntilElapsed(curatorConfig.getRetryUntilElapsed().getMaxElapsedTimeMs(),
                        curatorConfig.getRetryUntilElapsed().getSleepMsBetweenRetries());
                break;
            }
            default:
                throw new CuratorException("Invalid config value for retryType=" + curatorConfig.getRetryType());
        }

        create(curatorConfig.getNamespace(), curatorConfig.getConnectString(), curatorConfig.getSessionTimeoutMs(),
                curatorConfig.getConnectionTimeoutMs(), retryPolicy);

        startAndBlock();
    }

    /**
     * 创建ZooKeeper客户端实例
     *
     * @param connectString
     * @param sessionTimeoutMs
     * @param connectionTimeoutMs
     * @param retryPolicy
     */
    private void create(String namespace, String connectString, int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy) {
        log.info("Start to initialize Curator..");

        if (curatorFramework != null) {
            throw new CuratorException("Curator isn't null, it has been initialized already");
        }
        curatorFramework = CuratorFrameworkFactory.builder().namespace(namespace).connectString(connectString).sessionTimeoutMs(
                sessionTimeoutMs).connectionTimeoutMs(connectionTimeoutMs).retryPolicy(retryPolicy).build();
    }

    @Override
    public void start() {
        log.info("Start Curator...");

        validateClosedStatus();

        curatorFramework.start();
    }

    @Override
    public void startAndBlock() throws Exception {
        log.info("Start and block Curator...");

        validateClosedStatus();

        curatorFramework.start();
        curatorFramework.blockUntilConnected();
    }

    @Override
    public void startAndBlock(int maxWaitTime, TimeUnit units) throws Exception {
        log.info("Start and block Curator...");

        validateClosedStatus();

        curatorFramework.start();
        curatorFramework.blockUntilConnected(maxWaitTime, units);
    }

    @Override
    public void close() {
        log.info("Start to close Curator...");

        validateStartedStatus();

        curatorFramework.close();
    }

    @Override
    public boolean isInitialized() {
        return curatorFramework != null;
    }

    @Override
    public boolean isStarted() {
        return curatorFramework.getState() == CuratorFrameworkState.STARTED;
    }

    @Override
    public void validateStartedStatus() {
        if (curatorFramework == null) {
            throw new CuratorException("Curator isn't initialized");
        }

        if (!isStarted()) {
            throw new CuratorException("Curator is closed");
        }
    }

    @Override
    public void validateClosedStatus() {
        if (curatorFramework == null) {
            throw new CuratorException("Curator isn't initialized");
        }

        if (isStarted()) {
            throw new CuratorException("Curator is started");
        }
    }

    @Override
    public CuratorFramework getCurator() {
        return curatorFramework;
    }

    @Override
    public void addListener(ConnectionStateListener listener) {
        curatorFramework.getConnectionStateListenable().addListener(listener);
    }

    @Override
    public boolean pathExist(String path) throws Exception {
        return getPathStat(path) != null;
    }

    @Override
    public Stat getPathStat(String path) throws Exception {
        validateStartedStatus();
        PathUtils.validatePath(path);

        ExistsBuilder builder = curatorFramework.checkExists();
        if (builder == null) {
            return null;
        }

        Stat stat = builder.forPath(path);

        return stat;
    }

    @Override
    public void createPath(String path) throws Exception {
        validateStartedStatus();
        PathUtils.validatePath(path);

        curatorFramework.create().creatingParentsIfNeeded().forPath(path, null);
    }

    @Override
    public void createPath(String path, byte[] data) throws Exception {
        validateStartedStatus();
        PathUtils.validatePath(path);

        curatorFramework.create().creatingParentsIfNeeded().forPath(path, data);
    }

    @Override
    public void createPath(String path, CreateMode mode) throws Exception {
        validateStartedStatus();
        PathUtils.validatePath(path);

        curatorFramework.create().creatingParentsIfNeeded().withMode(mode).forPath(path, null);
    }

    @Override
    public void createPath(String path, byte[] data, CreateMode mode) throws Exception {
        validateStartedStatus();
        PathUtils.validatePath(path);

        curatorFramework.create().creatingParentsIfNeeded().withMode(mode).forPath(path, data);
    }

    @Override
    public void deletePath(String path) throws Exception {
        validateStartedStatus();
        PathUtils.validatePath(path);

        curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
    }

    @Override
    public List<String> getChildNameList(String path) throws Exception {
        validateStartedStatus();
        PathUtils.validatePath(path);

        return curatorFramework.getChildren().forPath(path);
    }

    @Override
    public List<String> getChildPathList(String path) throws Exception {
        List<String> childNameList = getChildNameList(path);

        List<String> childPathList = new ArrayList<String>();
        for (String childName : childNameList) {
            String childPath = path + "/" + childName;
            childPathList.add(childPath);
        }

        return childPathList;
    }

    @Override
    public String rootPath(String prefix) {
        return "/" + prefix;
    }

    @Override
    public String getPath(String prefix, String key) {
        return "/" + prefix + "/" + key;
    }
}
