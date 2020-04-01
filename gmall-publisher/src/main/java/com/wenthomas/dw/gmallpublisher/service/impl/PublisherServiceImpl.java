package com.wenthomas.dw.gmallpublisher.service.impl;

import com.wenthomas.dw.gmallpublisher.mapper.DauMapper;
import com.wenthomas.dw.gmallpublisher.mapper.OrderInfoMapper;
import com.wenthomas.dw.gmallpublisher.service.PublisherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-03-31 18:00
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    private static Logger logger = LoggerFactory.getLogger(PublisherServiceImpl.class);

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderInfoMapper orderInfoMapper;

    @Override
    public Long getDau(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDauList = dauMapper.getDauHour(date);

        Map<String, Long> resultMap = new HashMap<>();

        for (Map<String, Object> map : hourDauList) {
            String hour = (String) map.get("HOUR");
            Long count = (Long) map.get("COUNT");
            resultMap.put(hour, count);
            logger.info("{}: {}----{}", date, hour, count);
        }

        return resultMap;
    }

    /**
     * 查询当日总销售额
     * @param date
     * @return
     */
    @Override
    public Double getTotalAmount(String date) {
        Double result = orderInfoMapper.getTotalAmount(date);
        return result == null ? 0 : result;
    }

    /**
     * 查询当日每小时销售额明细
     * @param date
     * @return
     */
    @Override
    public Map<String, Double> getHourOrderAmount(String date) {
        Map<String, Double> result = new HashMap<>();
        List<Map<String, Object>> hourAmount = orderInfoMapper.getHourAmount(date);
        for (Map<String, Object> map : hourAmount) {
            String hour = (String) map.get("CREATE_HOUR");
            Double amount = ((BigDecimal) map.get("SUM")).doubleValue();
            result.put(hour, amount);
        }
        return result;
    }
}
