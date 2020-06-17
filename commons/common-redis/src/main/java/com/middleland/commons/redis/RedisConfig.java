package com.middleland.commons.redis;

import lombok.Data;

@Data
public class RedisConfig {

    private String host = "localhost";
    private Integer port = 6379;
    private String username = "";
    private String password = "";

}
