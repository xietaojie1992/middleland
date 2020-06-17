package com.middleland.commons.rabbit.models;

import com.middleland.commons.common.base.ICodeEnum;

/**
 * @author xietaojie
 */
public enum CompensateReason implements ICodeEnum {

    /**
     * CHANNEL 已关闭
     */
    CHANNEL_CLOSED("CHANNEL_CLOSED", "channel is closed"),

    RETURNED("RETURNED", ""),

    PUBLISH_EXCEPTION("PUBLISH_EXCEPTION", ""),

    UNCONFIRMED("UNCONFIRMED", "");

    private String code;
    private String message;

    CompensateReason(String code, String message) {
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

    public CompensateReason withReasonMsg(String message) {
        this.message = message;
        return this;
    }

    @Override
    public String toString() {
        return "CompensateReason{" + "code='" + code + '\'' + ", message='" + message + '\'' + '}';
    }
}
