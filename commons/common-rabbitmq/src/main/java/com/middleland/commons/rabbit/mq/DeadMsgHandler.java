package com.middleland.commons.rabbit.mq;

import com.middleland.commons.rabbit.models.Msg;

/**
 * 处理死信队列的消息
 *
 * @author xietaojie
 */
public interface DeadMsgHandler {

    void handle(Msg msg);

}
