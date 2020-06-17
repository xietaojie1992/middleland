package com.middleland.commons.rabbit.models;

import com.google.common.base.Preconditions;
import com.rabbitmq.client.BuiltinExchangeType;

import java.util.Map;

/**
 * @author xietaojie
 */
public class Exchange {

    private final String name;

    private final BuiltinExchangeType type;
    private final boolean             durable;
    private final boolean             autoDelete;
    private final Map<String, Object> arguments;

    private Exchange(Builder builder) {
        Preconditions.checkNotNull(builder.name);
        Preconditions.checkNotNull(builder.type);
        this.name = builder.name;
        this.type = builder.type;
        this.durable = builder.durable;
        this.autoDelete = builder.autoDelete;
        this.arguments = builder.arguments;
    }

    public String getName() {
        return name;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public BuiltinExchangeType getType() {
        return type;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String              name;
        private BuiltinExchangeType type       = BuiltinExchangeType.TOPIC;
        private boolean             durable    = true;
        private boolean             autoDelete = false;
        private Map<String, Object> arguments;

        private Builder() {
        }

        public Exchange build() {
            return new Exchange(this);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder type(BuiltinExchangeType type) {
            this.type = type;
            return this;
        }

        public Builder durable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public Builder autoDelete(boolean autoDelete) {
            this.autoDelete = autoDelete;
            return this;
        }

        public Builder arguments(Map<String, Object> arguments) {
            this.arguments = arguments;
            return this;
        }
    }
}
