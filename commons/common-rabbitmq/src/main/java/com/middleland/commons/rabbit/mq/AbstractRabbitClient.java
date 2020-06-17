package com.middleland.commons.rabbit.mq;

import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Preconditions;
import com.middleland.commons.rabbit.State;
import com.middleland.commons.rabbit.StateListener;
import com.middleland.commons.rabbit.models.Exchange;
import com.middleland.commons.rabbit.models.Msg;
import com.middleland.commons.rabbit.models.Queue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author xietaojie
 */
public abstract class AbstractRabbitClient {

    final static String                 DEFAULT_RK = "default.rk";
    final        AtomicReference<State> state      = new AtomicReference(State.LATENT);
    final        Type                   msgType    = new TypeReference<Msg>() {
    }.getType();

    private final    Queue      queue;
    private final    Exchange   exchange;
    private final    String     routingKey;
    private volatile Channel    channel;
    private          Connection connection;
    volatile         boolean    needRecover = true;

    AbstractRabbitClient(Queue queue, Exchange exchange, String routingKey) {
        Preconditions.checkNotNull(exchange, "Exchange Cannot be null");
        Preconditions.checkNotNull(routingKey, "RoutingKey Cannot be null");
        this.queue = queue;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    abstract Logger logger();

    public void start() throws IOException, TimeoutException {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");
        this.connection = connectionInit();
        this.channel = channelInit();
        logger().info("RabbitClient starting. Host={}, ConnectionName={}, ChannelNo={}, Exchange={}, RoutingKey={}",
                connection().getAddress().getHostAddress(), connection().getClientProvidedName(), channel().getChannelNumber(),
                exchange.getName(), routingKey);
        exchangeDeclare();
        queueDeclare();
        queueBind();
        addListeners();
        initDlx();
        extraElementInit();

        logger().info("RabbitClient started. Host={}, ConnectionName={}, ChannelNo={}, Exchange={}, RoutingKey={}",
                connection().getAddress().getHostAddress(), connection().getClientProvidedName(), channel().getChannelNumber(),
                exchange.getName(), routingKey);
    }

    /**
     * 当检测到 Channel 异常时，对 Channel 进行恢复
     */
    synchronized boolean recover() {
        logger().info("QueueName={}, Exchange={}, try to recover ...", queue != null ? queue.getName() : null,
                exchange != null ? exchange.getName() : null);
        if (needRecover && connection().isOpen() && !channel().isOpen()) {
            try {
                newChannel();
                exchangeDeclare();
                queueDeclare();
                queueBind();
                addListeners();
                initDlx();
                extraElementInit();
                logger().info("channel recovered. Host={}, ConnectionName={}, ChannelNo={}, Exchange={}, RoutingKey={}",
                        channel.getConnection().getAddress().getHostAddress(), channel.getConnection().getClientProvidedName(),
                        channel.getChannelNumber(), exchange.getName(), routingKey);
                return true;
            } catch (IOException e) {
                logger().error("channel recover failed. {}", e.getMessage());
                e.printStackTrace();
                return false;
            }
        } else {
            logger().info("no need to recover, needRecover={}, connection is {}, channel is {}", needRecover,
                    connection().isOpen() ? "open" : "close", channel().isOpen() ? "open" : "close");
        }
        return false;
    }

    void extraElementInit() throws IOException {
    }

    Channel channel() {
        return channel;
    }

    Connection connection() {
        return connection;
    }

    /**
     * Connection 初始化，Publisher 和 Subscriber 应该使用不同的 Connection
     *
     * @return
     */
    abstract Connection connectionInit() throws IOException, TimeoutException;

    /**
     * 使用 Connection 创建一个 Channel
     *
     * @return
     */
    private Channel channelInit() throws IOException {
        return connection().createChannel();
    }

    /**
     * 重新生成一个 Channel
     */
    private void newChannel() throws IOException {
        channel = connection().createChannel();
    }

    /**
     * 为 Channel 添加各种 Listener，如 ConfirmListener，ReturnListener，RecoveryListener，ShutdownListener 等
     */
    void addListeners() throws IOException {
        StateListener stateListener = new StateListener(
                connection().getClientProvidedName() + ".#" + channel().getChannelNumber() + "-" + (queue != null ? queue.getName()
                        : null)) {
            @Override
            public void onChannelShutdownByApplication() {
                logger().warn("{} onChannelShutdownByApplication, try to recover", getMark());
                if (needRecover) {
                    recover();
                } else {
                    logger().info("RabbitClient for queue[{}] is closed, no need to recover", queue);
                }
            }
        };
        ((Recoverable) channel()).addRecoveryListener(stateListener);
        channel().addShutdownListener(stateListener);
    }

    void initDlx() throws IOException {
        // do nothing, subclass needs to override.
    }

    /**
     * @throws IOException
     */
    void exchangeDeclare() throws IOException {
        channel().exchangeDeclare(exchange.getName(), exchange.getType(), exchange.isDurable(), exchange.isAutoDelete(),
                exchange.getArguments());
    }

    /**
     * 声明一个队列
     * <p>
     * durable, 是否持久化（true表示是，队列将在服务器重启时生存)
     * exclusive, 是否是独占队列（创建者可以使用的私有队列，断开后自动删除）
     * autoDelete, 当所有消费者客户端连接断开时是否自动删除队列
     */
    void queueDeclare() throws IOException {
        if (null == queue) {
            return;
        }
        channel().queueDeclare(queue.getName(), queue.isDurable(), queue.isExclusive(), queue.isAutoDelete(), queue.getArguments());
        if (queue.isEnableQos()) {
            channel.basicQos(queue.getPrefetchCount());
        }
    }

    /**
     * @throws IOException
     */
    void queueBind() throws IOException {
        if (null == queue) {
            return;
        }
        channel().queueBind(queue.getName(), exchange.getName(), routingKey);
    }

    public void close() throws IOException {
        logger().info("close channel, queue[{}], exchange[{}]", queue, exchange);
        state.set(State.CLOSED);
        needRecover = false;
        if (channel() != null && channel().isOpen()) {
            try {
                channel().close();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
}
