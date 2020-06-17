package com.middleland.commons.rabbit.models;

import com.middleland.commons.common.base.ICodeEnum;

/**
 * @author xietaojie
 */
public enum EMsgCode implements ICodeEnum {

    /**
     * 消息超时
     */
    EXPIRE("EXPIRE", "RabbitMQ Expire"),

    /**
     * 请求超时
     */
    TimeoutException("TimeoutException", ""),

    ChannelClosed("ChannelClosed", ""),

    ConnectionClosed("ConnectionClosed", ""),

    /**
     * 消息发布报错
     */
    IO_EXCEPTION("IO_EXCEPTION", ""),

    /**
     * 消息发布报错
     */
    SocketException("SocketException", ""),

    /**
     * 消息发布报错
     */
    SHUTDOWN_SIGNAL_EXCEPTION("SHUTDOWN_SIGNAL_EXCEPTION", ""),

    /**
     * RPC 返回结果无法识别
     */
    RESPONSE_UNKNOWN("RESPONSE_UNKNOWN", "");

    private String code;
    private String message;

    EMsgCode(String code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String code() {
        return this.code;
    }

    @Override
    public String message() {
        return this.message;
    }

    public EMsgCode withMsg(String message) {
        this.message = message;
        return this;
    }
}
