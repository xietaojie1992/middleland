package com.middleland.commons.common.base.templates;

import com.middleland.commons.common.base.NoRollbackException;
import com.middleland.commons.common.base.Result;
import com.middleland.commons.common.base.RollbackException;
import org.slf4j.Logger;

/**
 * @author xietaojie
 */
public class ExecutorTemplates {

    public static ExecutorTemplate normalExecutor() {

        return new ExecutorTemplate() {
            @Override
            public <T> void execute(Logger logger, Result<T> result, ExecuteTask task) {
                try {
                    task.checkParams();
                    task.execute();
                } catch (NoRollbackException ex) {
                    result.fail(ex.getExceptionCode(), ex.getExceptionMsg());
                } catch (RollbackException ex) {
                    logger.warn("rollback, RollbackException: {} - {}", ex.getExceptionCode(), ex.getExceptionMsg());
                    result.fail(ex.getExceptionCode(), ex.getExceptionMsg());
                    task.rollback();
                } catch (Exception e) {
                    logger.error("rollback, Exception:", e);
                    result.fail("ERROR", e.getMessage());
                    task.rollback();
                }
            }
        };
    }
}
