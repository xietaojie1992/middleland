package com.middleland.commons.common.base.templates;

/**
 * @author xietaojie
 */
public abstract class ExecuteTask {

    /**
     * 进行参数的检查
     */
    public void checkParams() {
    }

    /**
     * 具体执行
     */
    public abstract void execute();

    /**
     * 只是失败后进行回滚
     */
    public void rollback() {
    }
}
