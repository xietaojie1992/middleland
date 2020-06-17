package com.middleland.examples.rabbitmq;

import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.commons.rabbit.rpc.RpcClient;
import com.middleland.commons.rabbit.rpc.RpcReplyMsg;
import com.middleland.commons.rabbit.rpc.RpcRequestHandler;
import com.middleland.commons.rabbit.rpc.RpcRequestMsg;
import com.middleland.commons.rabbit.rpc.RpcServer;
import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xietaojie
 */
@Slf4j
public class TestRpcs extends ExamplesApplicationTests {

    private final static String  RPC_QUEUE       = "msgRpcQueue";
    private final static String  RPC_EXCHANGE    = "msgRpcExchange";
    private final static String  RPC_ROUTING_KEY = "msgRpcRoutingKey";
    private final static String  replyTo         = "replyTo";
    private final static Integer EXPIRE          = 5000;

    @Autowired
    private RabbitFactory rabbitFactory;

    @Test
    public void testRpc() throws IOException, TimeoutException {
        RpcServer rpcServer = new RpcServer(rabbitFactory.requireSubscribeConnection(), RPC_QUEUE, RPC_EXCHANGE, RPC_ROUTING_KEY,
                new RpcRequestHandler() {
                    @Override
                    public RpcReplyMsg handle(RpcRequestMsg request) {
                        log.info("RpcServer msgReceived, {}", request);
                        RpcReplyMsg rpcReply = new RpcReplyMsg();
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        rpcReply.succeed().withResultObj(request);
                        return rpcReply;
                    }
                });
        rpcServer.start();

        RpcClient rpcClient = new RpcClient(rabbitFactory.requirePublishConnection(), RPC_QUEUE, RPC_EXCHANGE, RPC_ROUTING_KEY, replyTo)
                .expire(1000);

        while (true) {
            RpcRequestMsg rpcRequest = new RpcRequestMsg();
            rpcRequest.setMsgType("request " + System.currentTimeMillis());
            rpcRequest.setTimestamp(System.currentTimeMillis());
            rpcRequest.setId(UUID.randomUUID().toString());
            RpcReplyMsg<RpcRequestMsg> rpcReply = rpcClient.call(rpcRequest);

            // 指定 RPC 超时时间
            rpcClient.call(rpcRequest);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void testRpcPressure() throws IOException, TimeoutException {
        RpcServer rpcServer = new RpcServer(rabbitFactory.requireSubscribeConnection(), RPC_QUEUE, RPC_EXCHANGE, RPC_ROUTING_KEY,
                new RpcRequestHandler() {
                    @Override
                    public RpcReplyMsg handle(RpcRequestMsg request) {
                        Thread thread = Thread.currentThread();
                        log.info("Thread, id={}, name={}", thread.getId(), thread.getName());
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        RpcReplyMsg rpcReply = new RpcReplyMsg();
                        rpcReply.setId(request.getId());
                        rpcReply.succeed().withResultObj(request);
                        return rpcReply;
                    }
                });
        rpcServer.setCurrency(5);
        rpcServer.start();

        RpcClient rpcClient = new RpcClient(rabbitFactory.requirePublishConnection(), RPC_QUEUE, RPC_EXCHANGE, RPC_ROUTING_KEY, replyTo);
        rpcClient.expire(3000);

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(20, 100, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1000));

        try {
            AtomicInteger atomicInteger = new AtomicInteger(0);
            int i = 0;
            while (++i <= 100) {
                threadPoolExecutor.execute(() -> {
                    RpcRequestMsg rpcRequest = new RpcRequestMsg();
                    rpcRequest.setMsgType("request " + System.currentTimeMillis());
                    rpcRequest.setTimestamp(System.currentTimeMillis());
                    rpcRequest.setId(String.valueOf(atomicInteger.incrementAndGet()));
                    rpcRequest.setEntity(new Object());
                    log.info("send request, {}", rpcRequest);
                    RpcReplyMsg<RpcRequestMsg> rpcReply = rpcClient.call(rpcRequest);
                });
            }
            Thread.sleep(10000);
            threadPoolExecutor.shutdown();
        } catch (Exception e) {
            log.error("", e);
        } finally {
            rpcClient.close();
            rpcServer.close();
        }

    }

    @Test
    public void testMultiServer() throws IOException, TimeoutException, InterruptedException {
        int count = 20;
        Map<Integer, RpcServer> serverMap = new HashMap<>(20);
        for (AtomicInteger i = new AtomicInteger(0); i.getAndIncrement() < count; ) {

            RpcServer rpcServer = new RpcServer(rabbitFactory.requireSubscribeConnection(), i.get() + "", i.get() + "", i.get() + "",
                    new RpcRequestHandler() {

                        @Override
                        public RpcReplyMsg handle(RpcRequestMsg request) {
                            Thread thread = Thread.currentThread();
                            log.info("Server-{}, Thread, id={}, name={}", i.get(), thread.getId(), thread.getName());
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            RpcReplyMsg rpcReply = new RpcReplyMsg();
                            rpcReply.setId(request.getId());
                            rpcReply.succeed().withResultObj(request);
                            return rpcReply;
                        }
                    });
            rpcServer.setCurrency(20);
            rpcServer.start();

            serverMap.put(i.get(), rpcServer);
        }

        Random random = new Random();
        while (true) {
            Thread.sleep(5000);
            int id = (random.nextInt(count - 1) + 1);
            RpcRequestMsg rpcRequest = new RpcRequestMsg();
            rpcRequest.setMsgType("request " + System.currentTimeMillis());
            rpcRequest.setTimestamp(System.currentTimeMillis());
            rpcRequest.setId(String.valueOf(id));
            rpcRequest.setEntity(new Object());
            log.info("send request, {}", rpcRequest);

            RpcClient rpcClient = new RpcClient(rabbitFactory.requirePublishConnection(), id + "", id + "", id + "", replyTo);
            rpcClient.expire(5000);
            RpcReplyMsg<RpcRequestMsg> rpcReply = rpcClient.call(rpcRequest);
            log.info("rpc time cost: {}, reply={}", System.currentTimeMillis() - rpcRequest.getTimestamp(), rpcReply);
        }

    }
}
