package com.wenthomas.dw.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.wenthomas.dw.gmallpublisher.bean.Option;
import com.wenthomas.dw.gmallpublisher.bean.SaleDetailResult;
import com.wenthomas.dw.gmallpublisher.bean.Stat;
import com.wenthomas.dw.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
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
     * http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米
     * @param date
     * @param keyWord
     * @param startPage
     * @param sizePerPage
     * @return
     * @throws IOException
     */
    @GetMapping("/sale_detail")
    public String saleDetail(@RequestParam("date") String date,
                             @RequestParam("keyWord") String keyWord,
                             @RequestParam("startPage") int startPage,
                             @RequestParam("sizePerPage") int sizePerPage) throws IOException {
        Map<String, Object> userGenderMap = publisherService.getSaleDetailAndAggGroupByFields(date,
                keyWord, startPage, sizePerPage, "user_gender", 2);

        Map<String, Object> userAgeMap = publisherService.getSaleDetailAndAggGroupByFields(date,
                keyWord, startPage, sizePerPage, "user_age", 100);

        SaleDetailResult saleInfo = new SaleDetailResult();
        saleInfo.setTotal((Integer) userGenderMap.get("total"));
        saleInfo.setDetail((List<Map<String, Object>>) userGenderMap.get("detail"));
        //饼图
        //性别饼图
        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        Map<String, Object> genderAgg = (Map<String, Object>) userGenderMap.get("agg");
        for (String key : genderAgg.keySet()) {
            Option opt = new Option();
            opt.setName(key.replace("F", "女").replace("M", "男"));
            opt.setValue((Long) genderAgg.get(key));
            genderStat.addOption(opt);
        }
        saleInfo.addStat(genderStat);
        //年龄饼图
        Stat ageStat = new Stat();
        ageStat.addOption(new Option("20岁以下", 0L));
        ageStat.addOption(new Option("20岁到30岁", 0L));
        ageStat.addOption(new Option("30岁及以上", 0L));
        genderStat.setTitle("用户年龄占比");
        Map<String, Object> ageAgg = (Map<String, Object>) userAgeMap.get("agg");
        for (String key : ageAgg.keySet()) {
            int age = Integer.parseInt(key);
            Long value = (Long) ageAgg.get(key);
            if (age < 20) {
                Option opt = ageStat.getOptions().get(0);
                opt.setValue(opt.getValue() + value);
            } else if (age < 30) {
                Option opt = ageStat.getOptions().get(1);
                opt.setValue(opt.getValue() + value);
            } else {
                Option opt = ageStat.getOptions().get(2);
                opt.setValue(opt.getValue() + value);
            }
        }
        saleInfo.addStat(ageStat);

        return JSON.toJSONString(saleInfo);
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
