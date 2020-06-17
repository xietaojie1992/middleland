package com.middleland.commons.common.base;

/**
 * @author xietaojie
 */
public class RollbackException extends RuntimeException {

    private String exceptionCode = "UNKNOWN_EXCEPTION";

    private String exceptionMsg = "default exception message";

    public RollbackException() {
        super();
    }

    public RollbackException(String exceptionCode) {
        super(exceptionCode);
        this.exceptionCode = exceptionCode;
    }

    public RollbackException(String exceptionCode, String exceptionMsg) {
        super(exceptionCode);
        this.exceptionCode = exceptionCode;
        this.exceptionMsg = exceptionMsg;
    }

    public RollbackException(ICodeEnum codeEnum) {
        super(codeEnum.code());
        this.exceptionCode = codeEnum.code();
        this.exceptionMsg = codeEnum.message();
    }

    public RollbackException(Throwable cause) {
        super(cause);
    }

    public RollbackException(String exceptionCode, Throwable cause) {
        super(exceptionCode, cause);
        this.exceptionCode = exceptionCode;
    }

    public RollbackException(ICodeEnum codeEnum, Throwable cause) {
        super(codeEnum.code(), cause);
        this.exceptionCode = codeEnum.code();
    }

    public RollbackException(String exceptionCode, String exceptionMsg, Throwable cause) {
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
