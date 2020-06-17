package com.middleland.commons.rabbit.models;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xietaojie
 */
public class Queue {

    private String  name;
    private boolean durable;
    private boolean exclusive;
    private boolean autoDelete;
    private boolean autoAck;
    private boolean enableDlx;
    private boolean enableQos;
    private String  dlx;
    private Integer prefetchCount;

    public String getName() {
        return name;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public boolean isEnableDlx() {
        return enableDlx;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public boolean isEnableQos() {
        return enableQos;
    }

    public String getDlx() {
        return dlx;
    }

    public Integer getPrefetchCount() {
        return prefetchCount;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    private Map<String, Object> arguments;

    private Queue(Builder builder) {
        Preconditions.checkNotNull(builder.name);
        this.name = builder.name;
        this.durable = builder.durable;
        this.exclusive = builder.exclusive;
        this.autoDelete = builder.autoDelete;
        this.autoAck = builder.autoAck;
        this.enableDlx = builder.enableDlx;
        this.enableQos = builder.enableQos;
        this.dlx = builder.dlx;
        this.arguments = new HashMap<>();

        if (enableDlx) {
            this.arguments.put("x-dead-letter-exchange", dlx);
        }
        if (enableQos) {
            this.autoAck = false;
            this.prefetchCount = builder.prefetchCount;
        }
        if (null != builder.expiration) {
            arguments.put("x-expires", builder.expiration);
        }
        if (builder.lazy) {
            this.arguments.put("x-queue-mode", "lazy");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String  name;
        private boolean durable    = true;
        private boolean exclusive  = false;
        private boolean autoDelete = false;
        private boolean autoAck    = false;
        private boolean enableDlx  = false;
        private boolean enableQos  = false;
        private boolean lazy       = false;
        private String  dlx;
        private Integer prefetchCount;
        private Integer expiration;

        private Builder() {
        }

        public Queue build() {
            return new Queue(this);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder durable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public Builder exclusive(boolean exclusive) {
            this.exclusive = exclusive;
            return this;
        }

        public Builder autoDelete(boolean autoDelete) {
            this.autoDelete = autoDelete;
            return this;
        }

        public Builder autoAck(boolean autoAck) {
            this.autoAck = autoAck;
            return this;
        }

        /**
         * 把 Dead Letter 转发到指定的 Exchange
         *
         * @param dlx
         * @return
         */
        public Builder enableDlx(String dlx) {
            this.enableDlx = true;
            this.dlx = dlx;
            return this;
        }

        /**
         * 开启消费端限流
         *
         * @param prefetchCount 当未确认的消息超过 prefetchCount，不进行新的消费
         */
        public Builder enableQos(int prefetchCount) {
            this.enableQos = true;
            this.prefetchCount = prefetchCount;
            return this;
        }

        /**
         * 设置队列中消息的存活时间
         *
         * @param expiration
         * @return
         */
        public Builder expires(Integer expiration) {
            this.expiration = expiration;
            return this;
        }

        /**
         * 设置为懒队列
         *
         * @return
         */
        public Builder lazy() {
            this.lazy = true;
            return this;
        }
    }
}
