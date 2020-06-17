package com.middleland.commons.rabbit.models;

import lombok.Data;

import java.io.Serializable;

/**
 * @author xietaojie
 */
@Data
public class Msg<T> implements Serializable {

    private static final long serialVersionUID = 253454665460039023L;

    private String id;
    private String publisher;
    private String msgType;
    private String handleType;
    private Long   timestamp;
    private T      entity;
}
