package com.middleland.examples.rabbitmq;

import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.examples.ExamplesApplicationTests;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author xietaojie
 */
@Slf4j
public class ConnectionTests extends ExamplesApplicationTests {

    @Autowired
    private RabbitFactory rabbitFactory;

    @Test
    public void test() throws IOException, TimeoutException {
        Connection connection = rabbitFactory.requirePublishConnection();
        log.info("Max channel: {}", connection.getChannelMax());

        Channel channel = connection.createChannel();

    }

}
