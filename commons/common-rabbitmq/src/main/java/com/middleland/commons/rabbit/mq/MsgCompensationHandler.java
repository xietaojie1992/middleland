package com.middleland.commons.rabbit.mq;

import com.middleland.commons.rabbit.models.CompensateReason;
import com.middleland.commons.rabbit.models.Msg;

/**
 * 当 Provider 发布的消息被 Confirmed with negative ack 或 Returned 时，Provider 需要对消息进行补偿
 *
 * @author xietaojie
 * @date 2019-12-30 10:36:50
 * @version $ Id: MsgCompensationHandler.java, v 0.1  xietaojie Exp $
 */
public interface MsgCompensationHandler {

    void compensate(Msg msg, CompensateReason reason);

}
