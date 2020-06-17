package com.middleland.commons.rabbit.mq;

import com.google.common.base.Preconditions;
import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.commons.rabbit.models.Exchange;
import com.middleland.commons.rabbit.models.Msg;
import com.middleland.commons.rabbit.models.Queue;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author xietaojie
 * @date 2020-03-09 11:53:18
 * @version $ Id: AbstractPulisherClient.java, v 0.1  xietaojie Exp $
 */
public abstract class AbstractPulisherClient extends AbstractRabbitClient {

    private final RabbitFactory rabbitFactory;

    AbstractPulisherClient(RabbitFactory rabbitFactory, Queue queue, Exchange exchange, String routingKey) {
        super(queue, exchange, routingKey);
        Preconditions.checkNotNull(rabbitFactory, "RabbitFactory Cannot be null");
        this.rabbitFactory = rabbitFactory;
    }

    AbstractPulisherClient(RabbitFactory rabbitFactory, Exchange exchange) {
        super(null, exchange, null);
        Preconditions.checkNotNull(rabbitFactory, "RabbitFactory Cannot be null");
        this.rabbitFactory = rabbitFactory;
    }

    @Override
    Connection connectionInit() throws IOException, TimeoutException {
        return rabbitFactory.requirePublishConnection();
    }

    public abstract boolean publish(Msg msg);

    public abstract boolean publish(Msg msg, BasicProperties props);

    abstract void addConfirmListener() throws IOException;

    abstract void addReturnListener();

    @Override
    void addListeners() throws IOException {
        addConfirmListener();
        addReturnListener();
        super.addListeners();
    }
}
