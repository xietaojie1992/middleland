package com.middleland.commons.rabbit;

/**
 * @author xietaojie
 */
public enum State {

    /**
     * 已准备
     */

    LATENT,
    /**
     * 一开始执行
     */
    STARTED,

    /**
     * 已关闭
     */
    CLOSED
}
