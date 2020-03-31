package com.wenthomas.dw.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-03-31 17:53
 */
public interface DauMapper {
    // 查询日活总数
    long getDauTotal(String date);

    // 查询小时明细
    List<Map<String, Object>> getDauHour(String date);
}
