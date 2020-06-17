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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author xietaojie
 */
@Slf4j
public class RabbitMqTests extends ExamplesApplicationTests {

    private final static String QUEUE    = "msgQ";
    private final static String EXCHANGE = "msgE";
    private final static String RK       = "msgR";

    @Autowired
    private RabbitFactory rabbitFactory;

    @Test
    public void testConnectionPool() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //connectionFactory.setUri();

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        for (int i = 0; i < 100; i++) {
            channel.exchangeDelete("msgQueue-" + i);
        }
        System.in.read();

        log.info("{}", connection.getClientProvidedName());
        log.info("{}", connection.getServerProperties());
    }

    @Test
    public void testMessaging() throws IOException, TimeoutException {
        Queue queue = Queue.builder().name(QUEUE).autoDelete(true).durable(false).exclusive(false).build();
        Exchange exchange = Exchange.builder().name(EXCHANGE).type(BuiltinExchangeType.TOPIC).durable(false).autoDelete(true).build();

        //监听执行队列的消息
        MsgSubscriber subscriber = new MsgSubscriber(rabbitFactory, queue, exchange, RK, msg -> {
            log.info("handle {}", msg);
            return new MsgHandleResult().succeed();
        });
        subscriber.start();

        // 推送消息到指定队列
        MsgPublisher msgPublisher = new MsgPublisher(rabbitFactory, queue, exchange, RK).compensate(
                (msg, reason) -> log.error("Msg need to compensated, {}, {}", msg, reason)).setMsgExpiration(10);
        msgPublisher.start();

        Msg request = new Msg();
        request.setMsgType("MsgType");
        request.setHandleType("HandleType");
        request.setTimestamp(System.currentTimeMillis());
        request.setEntity(new Object());

        int i = 0;
        while (true) {

            request.setId("" + (++i));
            msgPublisher.publish(request);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //System.in.read();
    }

}
