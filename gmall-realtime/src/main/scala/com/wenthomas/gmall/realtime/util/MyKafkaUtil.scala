package com.wenthomas.gmall.realtime.util

import com.wenthomas.gmall.common.Constant
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @author Verno
 * @create 2020-03-30 11:15 
 */
object MyKafkaUtil {

    private val params: Map[String, String] = Map[String, String](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProperty("kafka.broker.list"),
        ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getProperty("kafka.group")
    )

    def getKafkaStream(ssc: StreamingContext, topic: String, otherTopics: String*) = {
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            (otherTopics :+ topic).toSet
            //取kafka消息部分
        ).map(_._2)
    }

    /**
     * 测试
     * @param args
     */
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("MyKafkaUtil")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_EVENT)
        sourceStream.print(10)

        ssc.start()
        ssc.awaitTermination()
    }

}
