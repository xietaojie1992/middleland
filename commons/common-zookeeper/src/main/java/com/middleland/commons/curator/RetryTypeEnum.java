package com.middleland.commons.curator;

/**
 * @author xietaojie
 */
public enum RetryTypeEnum {

    /**
     * 重试指定的次数, 且每一次重试之间停顿的时间逐渐增加
     */
    EXPONENTIAL_BACKOFF_RETRY("exponentialBackoffRetry"),

    /**
     * 重试指定的次数, 且每一次重试之间停顿的时间逐渐增加，增加了最大重试次数的控制
     */
    BOUNDED_EXPONENTIAL_BACKOFF_RETRY("boundedExponentialBackoffRetry"),

    /**
     * 指定最大重试次数的重试
     */
    RETRY_NTIMES("retryNTimes"),

    /**
     * 永远重试
     */
    RETRY_FOREVER("retryForever"),

    /**
     * 一直重试，直到达到规定的时间一直重试，直到达到规定的时间
     */
    RETRY_UNTIL_ELAPSED("retryUntilElapsed");

    private String value;

    RetryTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static RetryTypeEnum fromString(String value) {
        for (RetryTypeEnum type : RetryTypeEnum.values()) {
            if (type.getValue().equalsIgnoreCase(value.trim())) {
                return type;
            }
        }

        throw new IllegalArgumentException("Mismatched type with value=" + value);
    }

    @Override
    public String toString() {
        return value;
    }
}