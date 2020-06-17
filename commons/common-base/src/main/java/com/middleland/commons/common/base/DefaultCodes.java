package com.middleland.commons.common.base;

/**
 * @author xietaojie
 */
public enum DefaultCodes implements ICodeEnum {

    SUCCESS("SUCCESS", "default success message"),
    FAILURE("FAILURE", "default failure message");

    private String code;
    private String message;

    DefaultCodes(String code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public String message() {
        return message;
    }
}
