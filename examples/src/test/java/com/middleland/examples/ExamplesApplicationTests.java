package com.middleland.examples;

import com.middleland.commons.common.base.Asserts;
import com.middleland.commons.common.base.DefaultCodes;
import com.middleland.commons.common.base.Result;
import com.middleland.commons.common.base.templates.ExecuteTask;
import com.middleland.commons.common.base.templates.ExecutorTemplates;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class ExamplesApplicationTests {

    @Test
    public void contextLoads() {
        Result result = new Result();
        ExecutorTemplates.normalExecutor().execute(log, result, new ExecuteTask() {
            @Override
            public void execute() {
                Asserts.assertFalseOrRollback(true, DefaultCodes.FAILURE);
            }
        });
    }

}
