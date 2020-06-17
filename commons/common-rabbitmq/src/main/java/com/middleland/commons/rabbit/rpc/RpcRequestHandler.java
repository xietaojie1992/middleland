package com.middleland.commons.rabbit.rpc;

/**
 * @author xietaojie
 */
public interface RpcRequestHandler {

    RpcReplyMsg handle(RpcRequestMsg request);

}