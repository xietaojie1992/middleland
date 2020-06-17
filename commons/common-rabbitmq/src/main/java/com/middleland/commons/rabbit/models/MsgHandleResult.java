package com.middleland.commons.rabbit.models;

/**
 * success, result of message handled.
 *
 * requeue, whether requeue or not where message handled failed.
 *
 * @author xietaojie
 */
public class MsgHandleResult {

    private boolean success = true;

    /**
     * true: 把消息重回队列
     * false: 把消息转达到指定 Dlx
     */
    private boolean requeue = false;

    public boolean isRequeue() {
        return requeue;
    }

    public boolean isSuccess() {
        return success;
    }

    public MsgHandleResult setRequeue(boolean requeue) {
        this.requeue = requeue;
        return this;
    }

    public MsgHandleResult succeed() {
        success = true;
        return this;
    }

    public MsgHandleResult failed() {
        success = false;
        return this;
    }

    @Override
    public String toString() {
        return "MsgHandleResult{" + "success=" + success + ", requeue=" + requeue + '}';
    }
}
