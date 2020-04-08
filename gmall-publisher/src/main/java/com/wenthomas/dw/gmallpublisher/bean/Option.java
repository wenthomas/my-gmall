package com.wenthomas.dw.gmallpublisher.bean;


/**
 * @author Verno
 * @create 2020-04-08 15:38
 */
public class Option {
    private String name;

    private Long value;

    public String getName() {
        return name;
    }

    public Option() {
    }

    public Option(String name, Long value) {
        this.name = name;
        this.value = value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
