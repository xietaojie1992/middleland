package com.middleland.examples.rabbitmq;

import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.commons.rabbit.models.Exchange;
import com.middleland.commons.rabbit.models.Msg;
import com.middleland.commons.rabbit.models.MsgHandleResult;
import com.middleland.commons.rabbit.models.Queue;
import com.middleland.commons.rabbit.mq.MsgPublisher;
import com.middleland.commons.rabbit.mq.MsgSubscriber;
import com.middleland.examples.ExamplesApplicationTests;
import com.rabbitmq.client.BuiltinExchangeType;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * @author xietaojie
 */
@Slf4j
public class TestFanoutExchange extends ExamplesApplicationTests {

    @Autowired
    private RabbitFactory rabbitFactory;

    @Test
    public void test() throws IOException, TimeoutException, InterruptedException {
        Queue queue1 = Queue.builder().name("queue-1").autoDelete(true).durable(false).exclusive(false).autoAck(true).build();
        Queue queue2 = Queue.builder().name("queue-2").autoDelete(true).durable(false).exclusive(false).autoAck(true).build();
        Queue queue3 = Queue.builder().name("queue-3").autoDelete(true).durable(false).exclusive(false).autoAck(true).build();
        Exchange exchange = Exchange.builder().name("exchange-fanout").type(BuiltinExchangeType.FANOUT).durable(false).autoDelete(true)
                .build();

        MsgSubscriber subscriber1 = new MsgSubscriber(rabbitFactory, queue1, exchange, msg -> {
            log.info("MsgSubscriber 1 ---> Msg received, {}", msg.getId());
            return new MsgHandleResult().succeed();
        });
        MsgSubscriber subscriber2 = new MsgSubscriber(rabbitFactory, queue2, exchange, msg -> {
            log.info("MsgSubscriber 2 ---> Msg received, {}", msg.getId());
            return new MsgHandleResult().succeed();
        });
        MsgSubscriber subscriber3 = new MsgSubscriber(rabbitFactory, queue3, exchange, msg -> {
            log.info("MsgSubscriber 3 ---> Msg received, {}", msg.getId());
            return new MsgHandleResult().succeed();
        });

        subscriber1.start();
        subscriber2.start();
        subscriber3.start();

        MsgPublisher msgPublisher = new MsgPublisher(rabbitFactory, exchange);
        msgPublisher.start();

        Msg msg = new Msg();
        msg.setId(UUID.randomUUID().toString());
        msg.setMsgType("test fanout");
        msg.setTimestamp(System.currentTimeMillis());
        for (int i = 0; i < 5; i++) {
            msgPublisher.publish(msg);
            Thread.sleep(5000);
        }
        System.in.read();
    }
}
