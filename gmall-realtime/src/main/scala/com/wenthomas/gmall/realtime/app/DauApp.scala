package com.wenthomas.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.wenthomas.gmall.common.Constant
import com.wenthomas.gmall.realtime.bean.StartupLog
import com.wenthomas.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-30 11:14 
 */
/*
redis去重的逻辑:
1. 把已经启动的设备id保存到redis中, 用set集合, 就可以只保留一个
set
key                                         value
"topic_startup:" + 2020-03-30               mid1,mid2
2. 对启动记录过滤, 已经启动过(redis中有记录)的不写到hbase中
    每3秒读一次

 */
object DauApp {

    def main(args: Array[String]): Unit = {
        //1，从kafka消费数据
        val conf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP)

        //1.1，把数据封装到样例类中，解析json字符串的时候，使用fastjson比较方便
        val startupLogStream = sourceStream.map(jsonStr => JSON.parseObject(jsonStr, classOf[StartupLog]))

        //2，过滤得到日活明细
        val firstStartupLogStream = startupLogStream.transform(rdd => {
            //从Redis中读取已经启动的设备集合
            val client = RedisUtil.getClient
            val key = Constant.TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val mids = client.smembers(key)
            client.close()
            //把已经启动的设备过滤掉，rdd中只留下那些redis中不存在的记录
            val midsBD = ssc.sparkContext.broadcast(mids)
            //考虑到某个mid在一个批次内可能启动多次，会出现重复情况，需要做数据清洗
            rdd
                    .filter(log => !midsBD.value.contains(log.mid))
                    .map(log => (log.mid, log))
                    .groupByKey()
                    .map({
                        case (_, it) =>
                            //同一批次内重复的只取其一
                            it.toList.minBy(_.ts)
                    })
        })
        import org.apache.phoenix.spark._
        //把第一次启动的设备信息保存在redis中
        firstStartupLogStream.foreachRDD(rdd => {
            rdd.foreachPartition(logIt => {
                //获取jedis连接
                val client = RedisUtil.getClient
                logIt.foreach(log => {
                    //每次向Set中存入一个mid
                    client.sadd(Constant.TOPIC_STARTUP + ":" + log.logDate, log.mid)
                })
                client.close()
            })


            //3，写到hbase：每个mid的每天的启动记录只有一条，使用redis去重
            rdd.saveToPhoenix(Constant.DAU_TABLE,
                Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
                zkUrl = Some("mydata01,mydata02,mydata03:2181"))
        })


        firstStartupLogStream.print(1000)
        ssc.start()
        ssc.awaitTermination()
    }

}
