package com.middleland.commons.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author xietaojie
 */
public interface CuratorInstance {

    /**
     * 启动 ZooKeeper 客户端
     */
    void start();

    /**
     * 启动 ZooKeeper 客户端，直到第一次连接成功
     *
     * @throws Exception
     */
    void startAndBlock() throws Exception;

    /**
     * 启动 ZooKeeper 客户端，直到第一次连接成功，为每一次连接配置超时
     *
     * @param maxWaitTime 最大等待次数，如果数值 <= 0，则无限次等待
     * @param units 每次等待的时间单元
     *
     * @throws Exception
     */
    void startAndBlock(int maxWaitTime, TimeUnit units) throws Exception;

    /**
     * 关闭 ZooKeeper 客户端连接
     */
    void close();

    /**
     * 查询 ZooKeeper 客户端是否初始化
     *
     * @return
     */
    boolean isInitialized();

    /**
     * 查询 ZooKeeper 客户端是否启动
     */
    boolean isStarted();

    /**
     * 检查 ZooKeeper 客户端是否已经初始化并启动正常
     *
     * 如果没有初始化和启动，则抛出异常
     */
    void validateStartedStatus();

    /**
     * 检查 ZooKeeper 客户端是否已经关闭
     */
    void validateClosedStatus();

    /**
     * 获取 ZooKeeper 客户端
     *
     * @return
     */
    CuratorFramework getCurator();

    /**
     * 添加监听器，监听 Connection 的状态
     * @param listener
     */
    void addListener(ConnectionStateListener listener);

    /**
     * 判断节点是否存在
     *
     * @param path
     * @return
     * @throws Exception
     */
    boolean pathExist(String path) throws Exception;

    /**
     * 获取指定路径的 ZNode stat
     *
     * @param path
     * @return
     * @throws Exception
     */
    Stat getPathStat(String path) throws Exception;

    /**
     * 创建节点
     *
     * @param path 节点的路径
     *
     * @throws Exception
     */
    void createPath(String path) throws Exception;

    /**
     * 创建节点，并写入数据
     *
     * @param path 节点的路径
     * @param data 该节点的数据
     *
     * @throws Exception
     */
    void createPath(String path, byte[] data) throws Exception;

    /**
     * 创建节点
     *
     * @param path 节点的路径
     * @param mode 节点的类型
     * @throws Exception
     */
    void createPath(String path, CreateMode mode) throws Exception;

    /**
     * 创建节点，并写入数据
     *
     * @param path 节点的路径
     * @param data 节点的类型
     * @param mode 该节点的数据
     *
     * @throws Exception
     */
    void createPath(String path, byte[] data, CreateMode mode) throws Exception;

    /**
     * 删除节点
     *
     * @param path 节点的路径
     *
     * @throws Exception
     */
    void deletePath(String path) throws Exception;

    /**
     * 获取子节点名称列表
     *
     * @param path 节点的路径
     * @return
     *
     * @throws Exception
     */
    List<String> getChildNameList(String path) throws Exception;

    /**
     * 获取子节点路径列表
     *
     * @param path 节点的路径
     * @return
     * @throws Exception
     */
    List<String> getChildPathList(String path) throws Exception;

    /**
     * 组装根节点路径
     *
     * @param prefix
     * @return
     */
    String rootPath(String prefix);

    /**
     * 组装节点路径
     *
     * @param prefix
     * @param key
     * @return
     */
    String getPath(String prefix, String key);
}
