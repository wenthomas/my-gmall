package com.wenthomas.dw.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.wenthomas.dw.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-03-31 18:07
 */
@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    /**
     * http://localhost:8070/realtime-total?date=2020-03-31
     *
     * [{"id":"dau","name":"新增日活","value":1200},
     * {"id":"new_mid","name":"新增设备","value":233} ]
     * @param date
     * @return
     */
    @GetMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        List<Map<String, String>> resultList = new ArrayList<>();

        //当日日活数据
        Map<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", publisherService.getDau(date).toString());
        resultList.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        resultList.add(map2);

        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        Double amount = publisherService.getTotalAmount(date);
        map3.put("value", amount.toString());
        resultList.add(map3);

        return JSON.toJSONString(resultList);
    }

    /**
     * http://localhost:8070/realtime-hour?id=dau&date=2020-03-31
     *
     *{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
     * "today":{"12":38,"13":1233,"17":123,"19":688 }}
     * @param id
     * @param date
     * @return
     */
    @GetMapping("realtime-hour")
    public String realtimeHour(@RequestParam("id") String id,
                               @RequestParam("date") String date) {
        if ("dau".equals(id)) {
            Map<String, Long> today = publisherService.getHourDau(date);
            Map<String, Long> yesterday = publisherService.getHourDau(getYesterday(date));

            Map<String, Map<String, Long>> resultMap = new HashMap<>();
            resultMap.put("today", today);
            resultMap.put("yesterday", yesterday);

            return JSON.toJSONString(resultMap);
        } else if ("order_amount".equals(id)) {
            Map<String, Double> today = publisherService.getHourOrderAmount(date);
            Map<String, Double> yesterday = publisherService.getHourOrderAmount(getYesterday(date));

            Map<String, Map<String, Double>> resultMap = new HashMap<>();
            resultMap.put("today", today);
            resultMap.put("yesterday", yesterday);

            return JSON.toJSONString(resultMap);
        }
        return "";
    }

    /**
     * 获取前一天日期
     * @param date
     * @return
     */
    private String getYesterday(String date) {
        return LocalDate.parse(date).plusDays(-1).toString();
    }

}
