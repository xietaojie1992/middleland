package com.middleland.commons.rabbit;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;

/**
 * @author xietaojie
 */
@Data
public class RabbitConfig {

    /**
     * 支持多个地址，当 Connection 出现异常时，可以进行重新恢复
     *
     */
    private List<String> addresses   = Lists.newArrayList("rabbitmq.internal");
    private Integer      port        = 5672;
    private String       host        = "localhost";
    private String       username    = "guest";
    private String       password    = "guest";
    private String       virtualHost = "/";

    /**
     * 连接池的数量，如果数量大于该值，则会复用 Connection
     */
    private Integer publishConnectionCount   = 2;
    private Integer subscribeConnectionCount = 2;

    /**
     * 心眺超时（秒），心跳包每隔 1/2 个超时单位发送一次
     */
    private Integer requestHeartbeat  = 5;
    private Integer connectionTimeout = 30;

    /**
     * default 5s
     */
    private Integer networkRecoveryInterval = 5000;

}
