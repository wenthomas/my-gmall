package com.wenthomas.dw.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Verno
 * @create 2020-04-08 15:33
 */
public class Stat {
    private String title;

    private List<Option> options = new ArrayList<>();

    /**
     * options的set方法改造为向集合添加元素
     * @param option
     */
    public void addOption(Option option) {
        options.add(option);
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    private void setOptions(List<Option> options) {
        this.options = options;
    }
}
