package com.middleland.commons.common.base.templates;

import com.middleland.commons.common.base.Result;

/**
 * @author xietaojie
 */
public interface ExecutorTemplate {

    <T> void execute(org.slf4j.Logger logger, Result<T> result, ExecuteTask task);

}
