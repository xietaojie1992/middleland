package com.middleland.examples.rabbitmq;

import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.commons.rabbit.models.Dlx;
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
public class TestTopicExchange extends ExamplesApplicationTests {

    @Autowired
    private RabbitFactory rabbitFactory;

    private final static String QUEUE_NAME_1  = "testTopicQueue_1";
    private final static String QUEUE_NAME_2  = "testTopicQueue_2";
    private final static String EXCHANGE_NAME = "testTopicExchange";
    private final static String RK            = "test.topic.rk.#";

    private final static String DLX = "testDLX";
    private final static String DLQ = "testDLX";

    @Test
    public void test() throws IOException, TimeoutException {

        Queue queue1 = Queue.builder().name(QUEUE_NAME_1).autoDelete(true).durable(false).exclusive(false).enableDlx(DLX).build();
        Queue queue2 = Queue.builder().name(QUEUE_NAME_2).autoDelete(true).durable(false).exclusive(false).enableDlx(DLX).build();
        Exchange exchange = Exchange.builder().name(EXCHANGE_NAME).type(BuiltinExchangeType.TOPIC).autoDelete(true).durable(false).build();
        Dlx dlx = Dlx.builder().queue(DLQ).exchange(DLX).deadMsgHandler(msg -> log.info("Dlx  ===> dead letter received, {}", msg.getId()))
                .build();

        MsgSubscriber subscriber1 = new MsgSubscriber(rabbitFactory, queue1, exchange, RK, msg -> {
            log.info("MsgSubscriber 1 ---> Msg received, {}", msg.getId());
            // 返回失败，测试 DLX 是否生效
            return new MsgHandleResult().failed();
        });

        MsgSubscriber subscriber2 = new MsgSubscriber(rabbitFactory, queue2, exchange, RK, msg -> {
            log.info("MsgSubscriber 2 ===> Msg received, {}", msg.getId());
            return new MsgHandleResult().succeed();
        });

        subscriber1.start();
        subscriber2.start();

        MsgPublisher msgPublisher = new MsgPublisher(rabbitFactory, queue1, exchange, RK);
        msgPublisher.enableDlx(dlx);
        msgPublisher.setMsgExpiration(1000);
        msgPublisher.compensate((msg, reason) -> log.info("msg compensate, {}, {}", msg, reason));
        msgPublisher.start();

        Msg msg = new Msg();
        msg.setId(UUID.randomUUID().toString());
        msg.setTimestamp(System.currentTimeMillis());
        msgPublisher.publish(msg);
        System.in.read();
    }
}
