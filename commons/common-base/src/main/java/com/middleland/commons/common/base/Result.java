package com.middleland.commons.common.base;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * @author xietaojie
 */
public class Result<T> implements Serializable {

    private static final long serialVersionUID = -941439279237547902L;

    private boolean success = false;

    private String code;

    private String message;

    private T obj = null;

    @Override
    public String toString() {
        return "Result{" + "success=" + success + ", code='" + code + '\'' + ", message='" + message + '\'' + ", obj=" + obj + '}';
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Result<T> succeed() {
        this.success = true;
        if (StringUtils.isBlank(this.code)) {
            this.code = "SUCCESS";
        }
        if (StringUtils.isBlank(this.message)) {
            this.message = "Operation Success.";
        }
        return this;
    }

    public Result<T> succeed(String code, String message) {
        this.success = true;
        this.code = code;
        this.message = message;
        return this;
    }

    public Result<T> succeed(ICodeEnum codeEnum) {
        this.success = true;
        this.code = codeEnum.code();
        this.message = codeEnum.message();
        return this;
    }

    public Result<T> succeed(ICodeEnum codeEnum, String message) {
        this.success = true;
        this.code = codeEnum.code();
        this.message = message;
        return this;
    }

    public Result<T> withSuccess(boolean success) {
        this.success = success;
        return this;
    }

    public Result<T> withCode(String code) {
        this.code = code;
        return this;
    }

    public Result<T> withMessage(String message) {
        this.message = message;
        return this;
    }

    public Result<T> withObj(T t) {
        this.obj = t;
        return this;
    }

    public Result<T> fail(Result other) {
        this.success = false;
        this.code = other.getCode();
        this.message = other.getMessage();
        return this;
    }

    public Result<T> fail(String code, String message) {
        this.success = false;
        this.code = code;
        this.message = message;
        return this;
    }

    public Result<T> fail(ICodeEnum e) {
        this.success = false;
        this.code = e.code();
        this.message = e.message();
        return this;
    }

    public Result<T> fail(ICodeEnum e, String message) {
        this.success = false;
        this.code = e.code();
        this.message = message;
        return this;
    }

    public String toJsonString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        sb.append("\"success\":" + "\"" + this.success + "\",");
        sb.append("\"code\":" + "\"" + this.code + "\",");
        sb.append("\"message\":" + "\"" + this.message + "\"");
        sb.append("}");
        return sb.toString();
    }

    public T getObj() {
        return obj;
    }

    public void setObj(T obj) {
        this.obj = obj;
    }

    public void copy(Result result) {
        this.success = result.isSuccess();
        this.code = result.getCode();
        this.message = result.getMessage();
    }

    public void copyAll(Result<T> result) {
        this.success = result.isSuccess();
        this.code = result.getCode();
        this.message = result.getMessage();
        this.obj = result.getObj();
    }

}
