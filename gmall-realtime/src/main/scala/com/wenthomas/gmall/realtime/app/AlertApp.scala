package com.wenthomas.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.wenthomas.gmall.common.{Constant, ESUtil}
import com.wenthomas.gmall.realtime.bean.{AlertInfo, EventLog}
import com.wenthomas.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks

/**
 * @author Verno
 * @create 2020-04-02 10:53 
 */
/**
 * 实时预警
 *
 * 需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
 * 并且在登录到领劵过程中没有浏览商品。同时达到以上要求则产生一条预警日志。
 * 同一设备，每分钟只记录一次预警。
 *
 * 1. 先从kafka消费数据
 *
 * 2. 数据封装
 *
 * 3. 实现需求
 *     1. 同一设备  按照设备id分组
 *     2. 5分钟内   window的概念: 窗口长度 窗口的滑动步长(6秒中统计一次)
 *     3. 三次及以上用不同账号登
 * 统计登录的用户数(聚合)
 *     4. 领取优惠劵
 * 过滤
 *     5. 领劵过程中没有浏览商品
 * 登录后没有浏览商品
 * -------
 *     6. 同一设备，每分钟只记录一次预警。
 * spark-steaming不实现, 交个es自己来实现
 * es中, id没分钟变化一次.
 *
 * 4. 把预警信息写入到es
 */
object AlertApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
        val ssc = new StreamingContext(conf, Seconds(3))

        //1，先从kafka消费数据：时间窗口每6s取5min内数据
        val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_EVENT).window(Minutes(5), Seconds(6))

        //2，数据封装
        val eventLogStream = sourceStream.map(s => JSON.parseObject(s, classOf[EventLog]))

        //3，实现需求
        //按照设备id进行分组
        val eventLogGrouped = eventLogStream.map(eventLog => (eventLog.mid, eventLog)).groupByKey

        //产生预警信息
        val alertInfoStream = eventLogGrouped.map({
            case (mid, eventLogs) =>
                //登陆过的设备集合
                val uidSet = new util.HashSet[String]()
                //5分钟内当前设备所有事件集合
                val eventList = new util.ArrayList[String]()
                //领取的优惠券对应的产品id
                val itemSet = new util.HashSet[String]()

                var isClickItem = false

                Breaks.breakable {
                    eventLogs.foreach(log => {
                        eventList.add(log.eventId)

                        log.eventId match {
                            case "coupon" =>
                                uidSet.add(log.uid)
                                itemSet.add(log.itemId)
                            case "clickItem" =>
                                //只要有一次浏览记录，就不该产生预警信息
                                isClickItem = true
                                Breaks.break
                            case _ =>
                            //其他事件类型默认不处理
                        }
                    })
                }

                //封装目标输出格式（Boolean, 预警信息）
                //设置预警条件
                (uidSet.size() >= 3 && !isClickItem, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))

        })
        alertInfoStream.print(1000)
        //4，把预警信息写入到es
        alertInfoStream
                        .filter(_._1)   //先把需要写入到es的预警信息过滤出来
                        .map(_._2)      //只保留预警信息
                        .foreachRDD(rdd => {
                            rdd.foreachPartition(alertInfos => {
                                //连接到es
                                //写数据
                                //关闭到es的连接
                                //同一设备，每分钟只记录一条预警，只写入一条记录到es
                                val data = alertInfos.map(info => (info.mid + ":" + info.ts / 1000 / 60, info))
                                ESUtil.insertBulk(Constant.INDEX_ALERT, data)
                            })
                        })


        ssc.start()
        ssc.awaitTermination()
    }

}
