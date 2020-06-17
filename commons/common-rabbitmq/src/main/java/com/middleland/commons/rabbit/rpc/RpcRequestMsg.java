package com.middleland.commons.rabbit.rpc;

import lombok.Data;

import java.io.Serializable;

/**
 * @author xietaojie
 */
@Data
public class RpcRequestMsg<T> implements Serializable {

    private static final long serialVersionUID = 4759826582562893L;

    private String id;

    private String msgType;

    private String handleType;

    private Long timestamp;

    private T entity;
}
