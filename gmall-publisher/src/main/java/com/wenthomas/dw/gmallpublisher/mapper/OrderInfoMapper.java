package com.wenthomas.dw.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-04-01 18:57
 */
public interface OrderInfoMapper {
    Double getTotalAmount(String date);

    List<Map<String, Object>> getHourAmount(String date);
}
