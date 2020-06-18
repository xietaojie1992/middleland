package com.middleland.commons.redis;

import lombok.Data;

@Data
public class RedisConfig {

    private String host = "localhost";
    private Integer port = 6379;
    private String username = "";
    private String password = "";

    private PoolConfig pool;

    @Data
    public static class PoolConfig {
        private Integer maxTotal = 8;
        private Integer maxIdle = 8;
        private Integer minIdle = 0;
        private Long maxWaitMillis = -1L;
    }

}
