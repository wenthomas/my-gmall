package com.wenthomas.dw.gmall.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @author Verno
 * @create 2020-04-01 12:01 
 */
object MyKafkaUtil {

    //配置kafka连接属性
    private val props = new Properties()
    props.setProperty("bootstrap.servers", "mydata01:9092,mydata02:9092,mydata03:9092")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //构建生产者
    private val producer = new KafkaProducer[String, String](props)

    /**
     * 生产kafka消息
     * @param topic
     * @param content
     */
    def send(topic: String, content: String) = {
        producer.send(new ProducerRecord[String, String](topic, content))
    }


}
