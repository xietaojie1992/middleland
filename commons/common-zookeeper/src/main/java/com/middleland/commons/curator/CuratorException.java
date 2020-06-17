package com.middleland.commons.curator;

/**
 * @author xietaojie
 * @date 2019-11-16 20:39:34
 * @version $ Id: CuratorException.java, v 0.1  xietaojie Exp $
 */
public class CuratorException extends RuntimeException {

    private static final long serialVersionUID = 123875478544582983L;

    public CuratorException() {
        super();
    }

    public CuratorException(String message) {
        super(message);
    }

    public CuratorException(String message, Throwable cause) {
        super(message, cause);
    }

    public CuratorException(Throwable cause) {
        super(cause);
    }
}