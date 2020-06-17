package com.middleland.examples;

import com.middleland.commons.rabbit.RabbitConfig;
import com.middleland.commons.rabbit.RabbitFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author xietaojie
 */
@Configuration
public class ExampleConfig {

    @Bean
    @ConfigurationProperties(prefix = "rabbitmq")
    public RabbitConfig rabbitConfig() {
        return new RabbitConfig();
    }

    @Bean
    @ConditionalOnBean(name = "rabbitConfig")
    public RabbitFactory rabbitFactory(RabbitConfig rabbitConfig) {
        return new RabbitFactory(rabbitConfig);
    }

    //@Bean
    //@ConfigurationProperties(prefix = "curator")
    //public CuratorConfig curatorConfig() {
    //    return new CuratorConfig();
    //}
    //
    //@Bean
    //@ConditionalOnBean(name = "curatorConfig")
    //public CuratorInstance curatorInstance(CuratorConfig curatorConfig) throws Exception {
    //    return new CuratorInstanceImpl(curatorConfig);
    //}
}
