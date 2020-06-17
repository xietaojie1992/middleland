package com.middleland.commons.rabbit;

import com.google.common.base.Preconditions;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.impl.ForgivingExceptionHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * RabbitMQ Connection Pool，Connection 对象创建的是 TCP 连接，创建和销毁本身都是很消耗资源的。
 * 解决办法就是把 Connection 缓存起来，一直不关闭，在传输数据时共享同一个 Connection 即可，即在一个 Connection 对象上创建多个 Channel 来实现数据传输。
 * <p>
 * 1.
 * <p>
 * TODO 后续优化：以防在 Connection 上创建过多的 Channel，可以限制 Channel 的数量，当单一 Connection 上创建的 Channel 超过一定量时，Connection会自动扩容
 *
 * @author xietaojie
 */
@Slf4j
public class RabbitFactory {

    private final List<Connection>  subscribeConnections;
    private final List<Connection>  publishConnections;
    private final ConnectionFactory connectionFactory;
    private final RabbitConfig      rabbitConfig;
    private final List<Address>     addressList;
    private final Object            subscribeMonitor;
    private final Object            publishMonitor;

    private final AtomicInteger subscribeCounter = new AtomicInteger(0);
    private final AtomicInteger publishCounter   = new AtomicInteger(0);

    public RabbitFactory(RabbitConfig rabbitConfig) {
        Preconditions.checkNotNull(rabbitConfig);
        this.rabbitConfig = rabbitConfig;
        this.subscribeMonitor = new Object();
        this.publishMonitor = new Object();
        this.connectionFactory = new ConnectionFactory();
        this.addressList = rabbitConfig.getAddresses().stream().map(h -> new Address(h.trim(), rabbitConfig.getPort())).collect(
                Collectors.toList());

        this.connectionFactory.setPort(rabbitConfig.getPort());
        this.connectionFactory.setHost(rabbitConfig.getHost());
        this.connectionFactory.setUsername(rabbitConfig.getUsername());
        this.connectionFactory.setPassword(rabbitConfig.getPassword());
        this.connectionFactory.setVirtualHost(rabbitConfig.getVirtualHost());
        this.connectionFactory.setRequestedHeartbeat(rabbitConfig.getRequestHeartbeat());
        this.connectionFactory.setConnectionTimeout(rabbitConfig.getConnectionTimeout());

        // Connection automatic recovery
        this.connectionFactory.setAutomaticRecoveryEnabled(true);
        // Retried after a fixed time interval (default is 5 seconds)
        this.connectionFactory.setNetworkRecoveryInterval(rabbitConfig.getNetworkRecoveryInterval());
        this.connectionFactory.setTopologyRecoveryEnabled(true);

        // using ForgivingExceptionHandler, only close channel where connection exception
        this.connectionFactory.setExceptionHandler(new ForgivingExceptionHandler());
        // default handler is StrictExceptionHandler which will close channel no mather what exception occurs.
        //this.connectionFactory.setExceptionHandler(new DefaultExceptionHandler());

        this.subscribeConnections = Collections.synchronizedList(new ArrayList<>(rabbitConfig.getSubscribeConnectionCount()));
        this.publishConnections = Collections.synchronizedList(new ArrayList<>(rabbitConfig.getPublishConnectionCount()));

        log.info("RabbitFactory initialed, config={}", rabbitConfig);
    }

    /**
     * If initial client connection to a RabbitMQ node fails, automatic connection recovery won't kick in.
     * Applications developers are responsible for retrying such subscribeConnections, logging failed attempts,
     * implementing a limit to the number of retries and so on.
     *
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    public Connection requireSubscribeConnection() throws IOException, TimeoutException {
        if (subscribeConnections.size() < rabbitConfig.getSubscribeConnectionCount()) {
            synchronized (subscribeMonitor) {
                if (subscribeConnections.size() < rabbitConfig.getSubscribeConnectionCount()) {
                    Connection connection = connectionFactory.newConnection(addressList, UUID.randomUUID().toString());
                    StateListener stateListener = new StateListener(connection.getClientProvidedName());
                    connection.addBlockedListener(stateListener);
                    connection.addShutdownListener(stateListener);
                    ((Recoverable) connection).addRecoveryListener(stateListener);
                    subscribeConnections.add(connection);
                    log.info("New subscriber connection created, name={}, connectionCount={}, poolSize={}",
                            connection.getClientProvidedName(), subscribeConnections.size(), rabbitConfig.getSubscribeConnectionCount());
                    return connection;
                }
            }
        }
        return subscribeConnections.get(subscribeCounter.getAndIncrement() % rabbitConfig.getSubscribeConnectionCount());
    }

    public Connection requirePublishConnection() throws IOException, TimeoutException {
        if (publishConnections.size() < rabbitConfig.getPublishConnectionCount()) {
            synchronized (publishMonitor) {
                if (publishConnections.size() < rabbitConfig.getPublishConnectionCount()) {
                    Connection connection = connectionFactory.newConnection(addressList, UUID.randomUUID().toString());
                    StateListener stateListener = new StateListener(connection.getClientProvidedName());
                    connection.addBlockedListener(stateListener);
                    connection.addShutdownListener(stateListener);
                    ((Recoverable) connection).addRecoveryListener(stateListener);
                    publishConnections.add(connection);
                    log.info("New publisher connection created, name={}, connectionCount={}, poolSize={}",
                            connection.getClientProvidedName(), publishConnections.size(), rabbitConfig.getPublishConnectionCount());
                    return connection;
                }
            }
        }
        return publishConnections.get(publishCounter.getAndIncrement() % rabbitConfig.getSubscribeConnectionCount());
    }

    public void close() {
        log.info("close all connections");
        try {
            subscribeConnections.forEach(c -> {
                if (c != null && c.isOpen()) {
                    try {
                        c.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            publishConnections.forEach(c -> {
                if (c != null && c.isOpen()) {
                    try {
                        c.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (Exception e) {
            log.error("RabbitFactory close exception", e);
        }

    }
}
