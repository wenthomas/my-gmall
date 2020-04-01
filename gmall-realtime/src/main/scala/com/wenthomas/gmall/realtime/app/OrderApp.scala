package com.wenthomas.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.wenthomas.gmall.common.Constant
import com.wenthomas.gmall.realtime.bean.OrderInfo
import com.wenthomas.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-04-01 17:16 
 */
/**
 * 从kafka中读取从mysql上传而来的实时业务数据，再写入到hbase中
 */
object OrderApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)

        //1，把读取到的json字符串进行解析，封装到样例类中
        val orderInfoStream = sourceStream.map(s => JSON.parseObject(s, classOf[OrderInfo]))
        //2，写入数据到hbase中

        //测试消费kafka数据，并检查是否脱敏数据
//        orderInfoStream.print(20)

        //使用phoenix方法需要导入相应包
        import org.apache.phoenix.spark._
        orderInfoStream.foreachRDD(rdd => {
            rdd.saveToPhoenix("GMALL_ORDER_INFO",
                Seq("ID",
                    "PROVINCE_ID",
                    "CONSIGNEE",
                    "ORDER_COMMENT",
                    "CONSIGNEE_TEL",
                    "ORDER_STATUS",
                    "PAYMENT_WAY",
                    "USER_ID",
                    "IMG_URL",
                    "TOTAL_AMOUNT",
                    "EXPIRE_TIME",
                    "DELIVERY_ADDRESS",
                    "CREATE_TIME",
                    "OPERATE_TIME",
                    "TRACKING_NO",
                    "PARENT_ORDER_ID",
                    "OUT_TRADE_NO",
                    "TRADE_BODY",
                    "CREATE_DATE",
                    "CREATE_HOUR"),
                zkUrl = Some("mydata01,mydata02,mydata03:2181"))
        })

        orderInfoStream.print(100)
        ssc.start()
        ssc.awaitTermination()
    }

}
