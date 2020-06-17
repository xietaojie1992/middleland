package com.middleland.commons.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryUntilElapsed;

/**
 * @author xietaojie
 */
public class RetryPolicyCreator {

    /**
     * 重试指定的次数, 且每一次重试之间停顿的时间逐渐增加
     *
     * @param baseSleepTimeMs 基础停顿时间
     * @param maxRetries 最大重试次数
     * @return
     */
    public static RetryPolicy createExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries) {
        return new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
    }

    /**
     * 重试指定的次数, 且每一次重试之间停顿的时间逐渐增加，增加了最大停顿时间的限制
     *
     * @param baseSleepTimeMs 基础停顿时间
     * @param maxSleepTimeMs 最大停顿时间
     * @param maxRetries 最大重试次数
     * @return
     */
    public static RetryPolicy createBoundedExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepTimeMs, int maxRetries) {
        return new BoundedExponentialBackoffRetry(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
    }

    /**
     * 指定最大重试次数的重试
     *
     * @param count 最大重试次数
     * @param sleepMsBetweenRetries 每次停顿时间
     * @return
     */
    public static RetryPolicy createRetryNTimes(int count, int sleepMsBetweenRetries) {
        return new RetryNTimes(count, sleepMsBetweenRetries);
    }

    /**
     * 永远重试
     *
     * @param retryIntervalMs 每次重试间隔时间
     * @return
     */
    public static RetryPolicy createRetryForever(int retryIntervalMs) {
        return new RetryForever(retryIntervalMs);
    }

    /**
     * 一直重试，直到达到了重试停止的规定时间
     *
     * @param maxElapsedTimeMs 重试停止的规定时间
     * @param sleepMsBetweenRetries 每次重试间隔时间
     * @return
     */
    public static RetryPolicy createRetryUntilElapsed(int maxElapsedTimeMs, int sleepMsBetweenRetries) {
        return new RetryUntilElapsed(maxElapsedTimeMs, sleepMsBetweenRetries);
    }

}
