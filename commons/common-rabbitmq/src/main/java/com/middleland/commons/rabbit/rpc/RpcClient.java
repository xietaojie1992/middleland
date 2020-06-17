package com.middleland.commons.rabbit.rpc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Preconditions;
import com.middleland.commons.common.base.NoRollbackException;
import com.middleland.commons.rabbit.State;
import com.middleland.commons.rabbit.StateListener;
import com.middleland.commons.rabbit.models.EMsgCode;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.BlockingCell;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.SocketException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author xietaojie
 */
@Slf4j
public class RpcClient {

    private final AtomicReference<State>            state            = new AtomicReference<>(State.LATENT);
    private final Map<String, BlockingCell<Object>> _continuationMap = new ConcurrentHashMap<>();
    private final Type                              msgType          = new TypeReference<RpcReplyMsg>() {
    }.getType();

    private final    Connection connection;
    private final    String     queueName;
    private final    String     exchange;
    private final    String     routingKey;
    private volatile String     replyTo;

    private          Channel channel;
    private          Integer expire      = 3000;
    private final    boolean mandatory   = true;
    private volatile int     correlationId;
    private volatile boolean needRecover = true;

    public RpcClient(Connection connection, String queueName, String exchange, String routingKey, String replyTo) throws IOException {
        Preconditions.checkNotNull(connection, "Connection Cannot be null");
        Preconditions.checkNotNull(queueName, "QueueName Cannot be null");
        Preconditions.checkNotNull(exchange, "Exchange Cannot be null");
        Preconditions.checkNotNull(routingKey, "RoutingKey Cannot be null");
        Preconditions.checkNotNull(replyTo, "replyTo Cannot be null");
        this.connection = connection;
        this.queueName = queueName;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.replyTo = replyTo;
        this.correlationId = 0;
        log.info("RpcClient initiating. QueueName={}, replyTo={}, expire={}", queueName, replyTo, expire);
        init();
    }

    public RpcClient(Connection connection, String queueName, String exchange, String routingKey) throws IOException {
        Preconditions.checkNotNull(connection, "Connection Cannot be null");
        Preconditions.checkNotNull(queueName, "QueueName Cannot be null");
        Preconditions.checkNotNull(exchange, "Exchange Cannot be null");
        Preconditions.checkNotNull(routingKey, "RoutingKey Cannot be null");
        Preconditions.checkNotNull(replyTo, "replyTo Cannot be null");
        this.connection = connection;
        this.queueName = queueName;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.correlationId = 0;
        log.info("RpcClient initiating. QueueName={}, expire={}", queueName, expire);
        init();
    }

    /**
     * 设置 RPC 超时时间。
     *
     * @param expire
     * @return
     */
    public RpcClient expire(Integer expire) {
        if (null == expire || expire <= 0) {
            throw new NoRollbackException("CONFIG_ERROR", "");
        }
        this.expire = expire;
        return this;
    }

    private synchronized void recover() {
        log.info("RpcClient, QueueName={} ,try to recover ...", queueName);
        if (needRecover && connection.isOpen() && !channel.isOpen()) {
            try {
                state.set(State.LATENT);
                init();
                needRecover = false;
            } catch (IOException e) {
                log.warn("recover failed.", e);
                e.printStackTrace();
            }
        } else {
            log.info("no need to recover, needRecover={}, connection is {}, channel is {}", needRecover,
                    connection.isOpen() ? "open" : "close", channel.isOpen() ? "open" : "close");
        }
    }

    private void init() throws IOException {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");
        Channel newChannel = connection.createChannel();

        this._continuationMap.clear();
        if (mandatory) {
            newChannel.addReturnListener(returnMessage -> {
                synchronized (_continuationMap) {
                    String replyId = returnMessage.getProperties().getCorrelationId();
                    BlockingCell<Object> blocker = _continuationMap.remove(replyId);
                    log.info("rpc replyId={}, TimeoutException, blockingCells={}", replyId, _continuationMap.size());
                    if (blocker == null) {
                        log.warn("No outstanding request for correlation ID {}", replyId);
                    } else {
                        blocker.set(new NoRollbackException("UnroutableRpcRequestException", returnMessage.toString()));
                    }
                }
            });
        }

        // 声明一个 rpc reply 匿名队列。不需要一个指定名称，因为不会有其他 client 可以使用这个队列（独占）
        //newChannel.queueDeclare(replyTo, false, true, true, null).getQueue();
        replyTo = newChannel.queueDeclare().getQueue();
        // 创建默认 Consumer，消费返回的数据 Rpc 消息
        newChannel.basicConsume(replyTo, true, new DefaultConsumer(newChannel) {
            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException signal) {
                synchronized (_continuationMap) {
                    for (Entry<String, BlockingCell<Object>> entry : _continuationMap.entrySet()) {
                        entry.getValue().set(signal);
                    }
                }
            }

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) {
                synchronized (_continuationMap) {
                    String replyId = properties.getCorrelationId();
                    BlockingCell<Object> blocker = _continuationMap.remove(replyId);
                    if (blocker == null) {
                        // Entry should have been removed if request timed out,
                        // log a warning nevertheless.
                        log.warn("No outstanding request for correlation ID {}", replyId);
                    } else {
                        blocker.set(JSON.parseObject(body, msgType));
                    }
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
                    log.info("RpcClient for queue[{}] is closed, no need to recover", queueName);
                }
            }
        };
        ((Recoverable) newChannel).addRecoveryListener(stateListener);
        newChannel.addShutdownListener(stateListener);
        log.info("RpcClient has been initiated. Host={}, ConnectionName={}, QueueName={}, replyTo={}, expire={}",
                newChannel.getConnection().getAddress().getHostAddress(), newChannel.getConnection().getClientProvidedName(), queueName,
                replyTo, expire);
        this.channel = newChannel;
    }

    /**
     * @param message
     * @return
     */
    public RpcReplyMsg call(RpcRequestMsg message) {
        return call(message, expire);
    }

    public RpcReplyMsg call(RpcRequestMsg message, Integer timeout) {

        if (!channel.isOpen()) {
            if (!channel.getConnection().isOpen()) {
                log.warn("Connection closed, {}", channel.getConnection().getClientProvidedName());
                return new RpcReplyMsg().fail(EMsgCode.ConnectionClosed.withMsg(channel.getConnection().getClientProvidedName()));
            }
            log.warn("Channel closed, id={}", channel.getChannelNumber());
            return new RpcReplyMsg().fail(EMsgCode.ChannelClosed.withMsg("channelId " + channel.getChannelNumber()));
        }

        long timeStamp = System.currentTimeMillis();
        message.setTimestamp(timeStamp);
        BlockingCell<Object> k = new BlockingCell<>();
        BasicProperties props;
        String corrId;
        synchronized (_continuationMap) {
            correlationId++;
            corrId = "" + correlationId;
            props = new BasicProperties.Builder().correlationId(corrId).expiration(timeout.toString()).replyTo(replyTo).build();
            _continuationMap.put(corrId, k);
            log.info("rpc msgId={}, correlationId={}", message.getId(), corrId);
        }
        try {
            channel.basicPublish(exchange, routingKey, mandatory, props, JSON.toJSONBytes(message));
        } catch (SocketException e) {
            _continuationMap.remove(corrId);
            return new RpcReplyMsg().fail(EMsgCode.SocketException.code(), e.getMessage());
        } catch (IOException e) {
            _continuationMap.remove(corrId);
            return new RpcReplyMsg().fail(EMsgCode.IO_EXCEPTION.code(), e.getMessage());
        }

        Object reply;
        try {
            reply = k.uninterruptibleGet(timeout);
        } catch (TimeoutException e) {
            log.info("rpc msgId={}, TimeoutException, blockingCells={}", message.getId(), _continuationMap.size());
            _continuationMap.remove(corrId);
            return new RpcReplyMsg().fail(EMsgCode.TimeoutException.code(), "RPC timeout");
        }
        log.info("rpc timeCost={}, msgId={}, correlationId={}, blockCells={}", System.currentTimeMillis() - timeStamp, corrId,
                message.getId(), _continuationMap.size());
        if (reply instanceof ShutdownSignalException) {
            ShutdownSignalException sig = (ShutdownSignalException) reply;
            ShutdownSignalException wrapper = new ShutdownSignalException(sig.isHardError(), sig.isInitiatedByApplication(),
                    sig.getReason(), sig.getReference());
            wrapper.initCause(sig);
            return new RpcReplyMsg().fail(EMsgCode.SHUTDOWN_SIGNAL_EXCEPTION.code(), wrapper.toString());
        } else if (reply instanceof RpcReplyMsg) {
            return (RpcReplyMsg) reply;
        } else {
            return new RpcReplyMsg().fail(EMsgCode.RESPONSE_UNKNOWN.code(), reply.toString());
        }
    }

    public void close() throws IOException {
        log.info("close channel");
        state.set(State.CLOSED);
        needRecover = false;
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public Channel getChannel() {
        return channel;
    }
}