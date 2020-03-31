package com.wenthomas.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wenthomas.gmall.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Verno
 * @create 2020-03-28 15:46
 */
@RestController
public class LoggerController {

    private static Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    KafkaTemplate template;

    @PostMapping("/log")
    public String logger(@RequestParam("log") String log) {
        //1，给数据加上时间戳
        log = addTs(log);

        //2，数据落盘
        saveToFile(log);
        //3，写入到kafka中
        sendtoKafka(log);
        return "";
    }
    /**
     * 将日志写入到kafka
     * 1，使用kafkaTemplate工具发送消息
     * @param log
     */
    private void sendtoKafka(String log) {
        String topic = Constant.TOPIC_STARTUP;
        if (log.contains("event")) {
            topic = Constant.TOPIC_EVENT;
        }
        try {
            template.send(topic, log);
            logger.info("topic={}, log={}", topic, log);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 日志的落盘，使用的log4j来完成
     * @param log
     */

    private void saveToFile(String log) {
        logger.info(log);
    }

    /**
     * 给日志加上时间戳：
     * 客户端的时间戳可能不统一，所以统一使用服务器时间戳
     * @param log   原始日志
     * @return  添加了时间戳的日志
     */
    private String addTs(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts", System.currentTimeMillis());
        return obj.toJSONString();
    }
}
