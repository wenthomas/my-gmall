package com.wenthomas.dw.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-04-08 15:28
 */
public class SaleDetailResult {
    private Integer total;

    private List<Map<String, Object>> detail;

    private List<Stat> stats = new ArrayList<>();

    /**
     * stats的set方法改造为向集合添加元素
     * @param stat
     */
    public void addStat(Stat stat) {
        stats.add(stat);
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<Map<String, Object>> getDetail() {
        return detail;
    }

    public void setDetail(List<Map<String, Object>> detail) {
        this.detail = detail;
    }

    public List<Stat> getStats() {
        return stats;
    }

    private void setStats(List<Stat> stats) {
        this.stats = stats;
    }
}
