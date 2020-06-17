package com.middleland.commons.rabbit.models;

import com.google.common.base.Preconditions;
import com.middleland.commons.rabbit.mq.DeadMsgHandler;

/**
 * @author xietaojie
 */
public class Dlx {

    private String         queue;
    private String         exchange;
    private String         routingKey;
    private DeadMsgHandler deadMsgHandler;

    public String getQueue() {
        return queue;
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public DeadMsgHandler getDeadMsgHandler() {
        return deadMsgHandler;
    }

    private Dlx(Builder builder) {
        Preconditions.checkNotNull(builder.queue);
        Preconditions.checkNotNull(builder.exchange);
        Preconditions.checkNotNull(builder.routingKey);
        Preconditions.checkNotNull(builder.deadMsgHandler);
        this.queue = builder.queue;
        this.exchange = builder.exchange;
        this.routingKey = builder.routingKey;
        this.deadMsgHandler = builder.deadMsgHandler;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String         queue;
        private String         exchange;
        private String         routingKey = "#";
        private DeadMsgHandler deadMsgHandler;

        private Builder() {
        }

        public Dlx build() {
            return new Dlx(this);
        }

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder routingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public Builder deadMsgHandler(DeadMsgHandler deadMsgHandler) {
            this.deadMsgHandler = deadMsgHandler;
            return this;
        }
    }
}
