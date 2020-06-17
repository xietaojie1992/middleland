package com.middleland.commons.rabbit.mq;

import com.google.common.base.Preconditions;
import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.commons.rabbit.models.Exchange;
import com.middleland.commons.rabbit.models.Queue;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消息订阅者，默认情况下 autoAck=false，qos=false, prefetchCount=20
 *
 * @author xietaojie
 */
@Slf4j
public class MsgSubscriber extends AbstractSubscriberClient {

    private final RabbitFactory rabbitFactory;

    public MsgSubscriber(RabbitFactory rabbitFactory, Queue queue, Exchange exchange, String routingKey, MsgHandler msgHandler) {
        super(rabbitFactory, queue, exchange, routingKey, msgHandler);
        Preconditions.checkNotNull(rabbitFactory, "RabbitFactory Cannot be null");
        this.rabbitFactory = rabbitFactory;
    }

    public MsgSubscriber(RabbitFactory rabbitFactory, Queue queue, Exchange exchange, MsgHandler msgHandler) {
        super(rabbitFactory, queue, exchange, msgHandler);
        Preconditions.checkNotNull(rabbitFactory, "RabbitFactory Cannot be null");
        this.rabbitFactory = rabbitFactory;
    }

    @Override
    Logger logger() {
        return log;
    }

    @Override
    Connection connectionInit() throws IOException, TimeoutException {
        return rabbitFactory.requireSubscribeConnection();
    }

}
