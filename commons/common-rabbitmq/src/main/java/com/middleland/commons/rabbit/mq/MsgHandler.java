package com.middleland.commons.rabbit.mq;

import com.middleland.commons.rabbit.models.Msg;
import com.middleland.commons.rabbit.models.MsgHandleResult;

/**
 * 消息监听器，向 MsgSubscriber 注册监听器
 *
 * @author xietaojie
 */
public interface MsgHandler {

    /**
     *
     * @param msg
     * @return requeue 是否把消息重回队列
     */
    MsgHandleResult handle(Msg msg);

}
