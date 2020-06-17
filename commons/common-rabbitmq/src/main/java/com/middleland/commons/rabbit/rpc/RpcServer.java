package com.middleland.commons.rabbit.rpc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.middleland.commons.rabbit.State;
import com.middleland.commons.rabbit.StateListener;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 监听 Client 的 RPC 消息，并处理返回结果，默认同时处理的并发量为 10，可以通过 setCurrency 方法来设置最大并发量。
 *
 * 结合了消息队列Qos机制和并发信号量机制，控制 RPC Server 的并发处理能力。
 *
 * @author xietaojie
 */
@Slf4j
public class RpcServer {

    private final AtomicReference<State> state   = new AtomicReference<>(State.LATENT);
    private final Type                   msgType = new TypeReference<RpcRequestMsg>() {
    }.getType();

    private final String                      queueName;
    private final String                      exchange;
    private final String                      routingKey;
    private final Connection                  connection;
    private       Channel                     channel;
    private final RpcRequestHandler           requestHandler;
    private final Semaphore                   semaphore;
    private final ThreadPoolExecutor          executor;
    private final ScheduledThreadPoolExecutor scheduledExecutor;

    private volatile boolean needRecover   = true;
    private          int     currency      = 20;
    private          int     prefetchCount = currency + 1;

    public RpcServer(Connection connection, String queueName, String exchange, String routingKey, RpcRequestHandler requestHandler)
            throws IOException {
        Preconditions.checkNotNull(connection, "Connection Cannot be null");
        Preconditions.checkNotNull(queueName, "QueueName Cannot be null");
        Preconditions.checkNotNull(exchange, "Exchange Cannot be null");
        Preconditions.checkNotNull(routingKey, "RoutingKey Cannot be null");
        this.connection = connection;
        this.queueName = queueName;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.requestHandler = requestHandler;
        this.semaphore = new Semaphore(currency, true);
        this.executor = new ThreadPoolExecutor(0, currency, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(prefetchCount),
                new ThreadFactoryBuilder().setNameFormat("RpcServer-thread-pool-queue-" + queueName).build());
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("RpcServer-scheduler-queue-" + queueName).build());

        scheduledExecutor.scheduleAtFixedRate(this::healthCheck, 60, 20, TimeUnit.SECONDS);
    }

    private synchronized void recover() {
        log.info("RpcServer, QueueName={} ,try to recover ...", queueName);
        if (needRecover && connection.isOpen() && !channel.isOpen()) {
            try {
                state.set(State.LATENT);
                start();
            } catch (IOException e) {
                log.warn("recover failed.", e);
                e.printStackTrace();
            }
        } else {
            log.info("no need to recover, needRecover={}, connection is {}, channel is {}", needRecover,
                    connection.isOpen() ? "open" : "close", channel.isOpen() ? "open" : "close");
        }
    }

    private void healthCheck() {
        if (needRecover && connection.isOpen() && !channel.isOpen()) {
            recover();
        } else {
            log.debug("health check, queue={}, connection is {}, channel is {}", queueName, connection.isOpen() ? "open" : "close",
                    channel.isOpen() ? "open" : "close");
        }
    }

    public void start() throws IOException {
        log.info("RpcServer starting. Host={}, ConnectionName={}, QueueName={}, prefetch={}, currency={}",
                connection.getAddress().getHostAddress(), connection.getClientProvidedName(), queueName, prefetchCount, currency);
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");
        Channel newChannel = connection.createChannel();
        // 声明交换器
        newChannel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);

        // 声明 RPC 队列
        newChannel.queueDeclare(queueName, false, false, false, null);

        // 进行绑定
        newChannel.queueBind(queueName, exchange, routingKey);

        // 比并发量数值要大，提高处理效率
        newChannel.basicQos(prefetchCount);

        newChannel.basicConsume(queueName, new DefaultConsumer(newChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                try {
                    RpcRequestMsg rpcRequest = JSON.parseObject(body, msgType);
                    log.info("RpcRequestMsg received, {} ", rpcRequest);
                    // 异步处理 RPC 请求
                    try {
                        semaphore.acquire();
                        executor.execute(() -> {
                            try {
                                log.info("handle rpc request {}", rpcRequest.getId());
                                RpcReplyMsg rpcReply = requestHandler.handle(rpcRequest);
                                log.info("handle rpc request {}, result={}", rpcRequest.getId(), rpcReply);
                                BasicProperties replyProps = new BasicProperties.Builder().correlationId(properties.getCorrelationId())
                                        .build();
                                getChannel().basicPublish("", properties.getReplyTo(), replyProps, JSON.toJSONBytes(rpcReply));
                            } catch (IOException e) {
                                log.error("IOException", e);
                            } finally {
                                semaphore.release();
                            }
                        });
                    } catch (InterruptedException e) {
                        log.error("InterruptedException", e);
                    } catch (Exception e) {
                        log.error("Error occurred when handling delivery, ", e);
                    }
                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                } catch (Exception e) {
                    log.error("Error occurred when handling delivery, ", e);
                }
            }
        });

        StateListener stateListener = new StateListener(connection.getClientProvidedName() + ".#" + newChannel.getChannelNumber()) {
            @Override
            public void onChannelShutdownByApplication() {
                log.warn("{} onChannelShutdownByApplication, try to recover", getMark());
                if (needRecover) {
                    recover();
                } else {
                    log.info("RpcServer for queue[{}] is closed, no need to recover", queueName);
                }
            }
        };
        ((Recoverable) newChannel).addRecoveryListener(stateListener);
        newChannel.addShutdownListener(stateListener);
        log.info("RpcServer started. Host={}, ConnectionName={}, ChannelNo={}, QueueName={}, prefetch={}, currency={}",
                newChannel.getConnection().getAddress().getHostAddress(), newChannel.getConnection().getClientProvidedName(),
                newChannel.getChannelNumber(), queueName, prefetchCount, currency);
        this.channel = newChannel;
    }

    /**
     * 设置 RPC Server 处理请求的最大并发量。
     *
     * 注：必须要在 start 之前设置
     *
     * @param currency 最大并发量
     * @return
     */
    public RpcServer setCurrency(int currency) {
        this.currency = currency;
        this.prefetchCount = currency + 1;
        return this;
    }

    public void close() throws IOException {
        log.info("close channel");
        state.set(State.CLOSED);
        needRecover = false;
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
        log.info("shutdown thread pool");
        scheduledExecutor.shutdown();
        executor.shutdown();
    }

    public Connection getConnection() {
        return connection;
    }

    public Channel getChannel() {
        return channel;
    }

}