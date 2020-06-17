package com.middleland.commons.rabbit.mq;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.commons.rabbit.models.Exchange;
import com.middleland.commons.rabbit.models.Msg;
import com.middleland.commons.rabbit.models.MsgHandleResult;
import com.middleland.commons.rabbit.models.Queue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author xietaojie
 */
public abstract class AbstractSubscriberClient extends AbstractRabbitClient {

    private final RabbitFactory               rabbitFactory;
    private final MsgHandler                  msgHandler;
    private final Queue                       queue;
    private final ScheduledThreadPoolExecutor scheduledExecutor;

    AbstractSubscriberClient(RabbitFactory rabbitFactory, Queue queue, Exchange exchange, String routingKey, MsgHandler msgHandler) {
        super(queue, exchange, routingKey);
        Preconditions.checkNotNull(rabbitFactory, "RabbitFactory Cannot be null");
        this.rabbitFactory = rabbitFactory;
        this.msgHandler = msgHandler;
        this.queue = queue;
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("RpcServer-scheduler-queue-" + queue.getName()).build());
        scheduledExecutor.scheduleAtFixedRate(this::healthCheck, 60, 20, TimeUnit.SECONDS);
    }

    AbstractSubscriberClient(RabbitFactory rabbitFactory, Queue queue, Exchange exchange, MsgHandler msgHandler) {
        this(rabbitFactory, queue, exchange, DEFAULT_RK, msgHandler);
    }

    private void healthCheck() {
        if (needRecover && connection().isOpen() && !channel().isOpen()) {
            recover();
        } else {
            logger().debug("health check, queue={}, connection is {}, channel is {}", queue.getName(),
                    connection().isOpen() ? "open" : "close", channel().isOpen() ? "open" : "close");
        }
    }

    void basicConsume() throws IOException {
        channel().basicConsume(queue.getName(), queue.isAutoAck(), new DefaultConsumer(channel()) {

            @Override
            public void handleRecoverOk(String consumerTag) {
                logger().warn("MsgSubscriber, consumer recoverOk, consumerTag={}, Queue={}", consumerTag, queue.getName());
            }

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                logger().warn("MsgSubscriber, consumer shutdown, consumerTag={}, Queue={}", consumerTag, queue.getName());
            }

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                try {
                    Msg msg = JSON.parseObject(body, msgType);
                    logger().info("Msg received in Queue[{}] -> {}", queue.getName(), msg);
                    MsgHandleResult result = msgHandler.handle(msg);
                    if (queue.isAutoAck()) {
                        return;
                    }
                    if (result != null) {
                        if (result.isSuccess()) {
                            // tells broker that this msg has been handled
                            channel().basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            // FIXME warning, would msg always been requeued ?
                            logger().warn("msg handle failed, reject and {}", result.isRequeue() ? "requeued" : "discarded/dead-lettered");
                            channel().basicNack(envelope.getDeliveryTag(), false, result.isRequeue());
                        }
                    } else {
                        // by default, tells broker that this msg has been handled
                        channel().basicAck(envelope.getDeliveryTag(), false);
                    }
                } catch (Exception e) {
                    logger().error("Error occurred when handling delivery, ", e);
                }
            }
        });
    }

    void qos() throws IOException {
        if (queue.isEnableQos()) {
            channel().basicQos(queue.getPrefetchCount());
        }
    }

    @Override
    void initDlx() throws IOException {
        if (queue.isEnableDlx()) {
            logger().info("enable Dlx, exchange={}", queue.getDlx());
            channel().exchangeDeclare(queue.getDlx(), BuiltinExchangeType.TOPIC, true, false, null);
        }
    }

    @Override
    void extraElementInit() throws IOException {
        qos();
        basicConsume();
    }

    @Override
    Connection connectionInit() throws IOException, TimeoutException {
        return rabbitFactory.requireSubscribeConnection();
    }

    @Override
    public void close() throws IOException {
        scheduledExecutor.shutdown();
        super.close();
    }
}
