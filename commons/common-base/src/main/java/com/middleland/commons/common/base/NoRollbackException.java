package com.middleland.commons.common.base;

/**
 * @author xietaojie
 */
public class NoRollbackException extends RuntimeException {

    private String exceptionCode = "UNKNOWN_EXCEPTION";

    private String exceptionMsg = "default exception message";

    public NoRollbackException() {
        super();
    }

    public NoRollbackException(String exceptionCode) {
        super(exceptionCode);
        this.exceptionCode = exceptionCode;
    }

    public NoRollbackException(String exceptionCode, String exceptionMsg) {
        super(exceptionCode);
        this.exceptionCode = exceptionCode;
        this.exceptionMsg = exceptionMsg;
    }

    public NoRollbackException(ICodeEnum codeEnum) {
        super(codeEnum.code());
        this.exceptionCode = codeEnum.code();
        this.exceptionMsg = codeEnum.message();
    }

    public NoRollbackException(Throwable cause) {
        super(cause);
    }

    public NoRollbackException(String exceptionCode, Throwable cause) {
        super(exceptionCode, cause);
        this.exceptionCode = exceptionCode;
    }

    public NoRollbackException(ICodeEnum codeEnum, Throwable cause) {
        super(codeEnum.code(), cause);
        this.exceptionCode = codeEnum.code();
    }

    public NoRollbackException(String exceptionCode, String exceptionMsg, Throwable cause) {
        super(exceptionCode, cause);
        this.exceptionCode = exceptionCode;
        this.exceptionMsg = exceptionMsg;
    }

    public String getExceptionCode() {
        return exceptionCode;
    }

    public void setExceptionCode(String exceptionCode) {
        this.exceptionCode = exceptionCode;
    }

    public String getExceptionMsg() {
        return exceptionMsg;
    }

    public void setExceptionMsg(String exceptionMsg) {
        this.exceptionMsg = exceptionMsg;
    }

}
