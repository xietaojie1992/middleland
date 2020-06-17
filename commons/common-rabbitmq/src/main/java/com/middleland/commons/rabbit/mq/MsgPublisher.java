package com.middleland.commons.rabbit.mq;

import com.alibaba.fastjson.JSON;
import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.commons.rabbit.models.CompensateReason;
import com.middleland.commons.rabbit.models.Dlx;
import com.middleland.commons.rabbit.models.Exchange;
import com.middleland.commons.rabbit.models.Msg;
import com.middleland.commons.rabbit.models.Queue;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author xietaojie
 */
@Slf4j
public class MsgPublisher extends AbstractPulisherClient {

    private final SortedMap<Long, Msg> pendingConfirms = new ConcurrentSkipListMap<>();
    private final Exchange             exchange;
    private final String               routingKey;
    private       BasicProperties      properties;
    private       Dlx                  dlx;
    private       int                  msgExpiration   = -1;

    /**
     * 启用 ReturnListener 必须要设置为 true
     */
    private final boolean mandatory = true;

    private MsgCompensationHandler msgCompensationHandler;

    public MsgPublisher(RabbitFactory rabbitFactory, Queue queue, Exchange exchange, String routingKey) {
        super(rabbitFactory, queue, exchange, routingKey);
        this.exchange = exchange;
        this.routingKey = routingKey;

        if (msgExpiration > 0) {
            this.properties = new BasicProperties.Builder().expiration(String.valueOf(msgExpiration)).build();
        }
    }

    public MsgPublisher(RabbitFactory rabbitFactory, Exchange exchange) {
        super(rabbitFactory, null, exchange, DEFAULT_RK);
        this.exchange = exchange;
        this.routingKey = DEFAULT_RK;

        if (msgExpiration > 0) {
            this.properties = new BasicProperties.Builder().expiration(String.valueOf(msgExpiration)).build();
        }
    }

    @Override
    Logger logger() {
        return log;
    }

    @Override
    void initDlx() throws IOException {
        if (null != dlx) {
            channel().exchangeDeclare(dlx.getExchange(), BuiltinExchangeType.TOPIC, true, false, null);
            channel().queueDeclare(dlx.getQueue(), true, false, false, null);
            channel().queueBind(dlx.getQueue(), dlx.getExchange(), dlx.getRoutingKey());
            channel().basicConsume(dlx.getQueue(), true, new DefaultConsumer(channel()) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                        throws IOException {
                    Msg msg = JSON.parseObject(body, msgType);
                    log.info("DeadLetter msg received in Queue[{}] -> {}", dlx.getQueue(), msg);
                    dlx.getDeadMsgHandler().handle(msg);
                }
            });
        }
    }

    @Override
    public boolean publish(Msg msg) {
        return publish(msg, this.properties);
    }

    @Override
    public boolean publish(Msg msg, BasicProperties props) {
        if (channel().isOpen()) {
            msg.setTimestamp(System.currentTimeMillis());
            long sequenceNo = channel().getNextPublishSeqNo();
            try {
                addMsg(sequenceNo, msg);
                channel().basicPublish(exchange.getName(), routingKey, mandatory, props, JSON.toJSONBytes(msg));
            } catch (IOException e) {
                e.printStackTrace();
                processAck(sequenceNo, false, CompensateReason.PUBLISH_EXCEPTION.withReasonMsg(e.getMessage()));
            }
            log.info("sequenceNo={}, msg[{}] published. Exchange={}, RoutingKey={}.", msg, sequenceNo, exchange.getName(), routingKey);
            return true;
        } else {
            log.error("channel is close, connection[{}] is {}. Exchange={}, RoutingKey={}.", connection().getClientProvidedName(),
                    channel().getConnection().isOpen() ? "open" : "close", exchange.getName(), routingKey);
            //TODO 补偿？可能会导致网络异常的情况下，产生的消息风暴
            if (recover()) {
                // 当 Channel 恢复后，尝试再次发送消息
                log.info("republish msg");
                return publish(msg, props);
            } else {
                doCompensation(msg, CompensateReason.CHANNEL_CLOSED);
            }
            return false;
        }
    }

    @Override
    void addConfirmListener() throws IOException {
        channel().confirmSelect();
        channel().addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                // 消息已经送达 Exchange 并被持久化
                log.info("ack, deliveryTag={}", deliveryTag);
                processAck(deliveryTag, true, null);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                log.info("nack, deliveryTag={}", deliveryTag);
                processAck(deliveryTag, false, CompensateReason.UNCONFIRMED);
                //FIXME 保障消息发布的可靠性，可以通过 ①消息落库，或者 ②延迟消息 两种方式
            }
        });
    }

    @Override
    void addReturnListener() {
        channel().addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
            Msg msg = JSON.parseObject(body, Msg.class);
            log.warn("msg returned, replyCode={}, replyText={}, exchange={}, routingKey={}, properties={}, body={}", replyCode, replyText,
                    exchange, routingKey, properties, msg);
            doCompensation(msg, CompensateReason.RETURNED.withReasonMsg(replyText));
        });
    }

    private synchronized void processAck(long seq, boolean ack, CompensateReason reason) {
        Msg msg = pendingConfirms.remove(seq);
        if (msg == null) {
            log.warn("msg[{}] not found, ack={}", seq, ack);
            return;
        }
        if (!ack) {
            // msg not confirmed, need to be compensated
            doCompensation(msg, reason);
        }
    }

    private synchronized void addMsg(long sequenceNo, Msg msg) {
        pendingConfirms.put(sequenceNo, msg);
    }

    /**
     * FIXME 消息确认机制的过期处理
     */
    private synchronized void expire(long expireTime) {
        log.info("clean expired message to be confirmed");
        Set<Long> seqSet = pendingConfirms.keySet();
        seqSet.forEach(s -> {
            Msg msg = pendingConfirms.get(s);
            if (msg != null && msg.getTimestamp() < expireTime) {
                log.info("{} is expired.", msg);
                pendingConfirms.remove(s);
            } else {// 当序号最小的都没有过期，则不需要继续检查
                return;
            }
        });
    }

    private void doCompensation(Msg msg, CompensateReason reason) {
        log.info("doCompensation, {}, {}", msg, reason);
        if (msgCompensationHandler != null) {
            msgCompensationHandler.compensate(msg, reason);
        }
    }

    /**
     * 设置单条消息的存活时间
     *
     * 注：如果队列也设置了消息存活时间，以小的为准
     *
     * @param msgExpiration
     * @return
     */
    public MsgPublisher setMsgExpiration(int msgExpiration) {
        this.msgExpiration = msgExpiration;
        return this;
    }

    /**
     * 监听没有到达 Exchange 或 Queue 的消息，进行补偿
     *
     * @param handler
     * @return
     */
    public MsgPublisher compensate(MsgCompensationHandler handler) {
        this.msgCompensationHandler = handler;
        return this;
    }

    /**
     * 启动死信队列机制，生产者监听死信队列，并对消息补偿
     *
     * @return
     */
    public MsgPublisher enableDlx(Dlx dlx) {
        this.dlx = dlx;
        return this;
    }

}
