package com.wenthomas.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.wenthomas.gmall.common.Constant
import com.wenthomas.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.wenthomas.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis


/**
 * @author Verno
 * @create 2020-04-07 14:34 
 */
object SaleDetailApp {

    def saveToRedis(client: Jedis, key: String, value: AnyRef) = {
        //样例类不能用fastJson来解析，使用Serialization.write
        import org.json4s.DefaultFormats
        val jsonString = Serialization.write(value)(DefaultFormats)
        client.set(key, jsonString)
    }

    def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo)= {
        val key = "order_info:" + orderInfo.id
        saveToRedis(client, key, orderInfo)
    }

    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail) = {
        val key = s"order_info:${orderDetail.order_id}:${orderDetail.id}"
        saveToRedis(client, key, orderDetail)
    }

    /**
     * 对传入的两个流进行full join，需要满足网络延迟导致的当前一批数据非完整订单数据，需要等待直至完整封装orderInfo和orderDetail的场景
     *
     * @param orderInfoStream
     * @param orderDetailStream
     * @return
     */
    def fullJoin(orderInfoStream: DStream[(String, OrderInfo)], orderDetailStream: DStream[(String, OrderDetail)]) = {
        val stream = orderInfoStream.fullOuterJoin(orderDetailStream)
                .mapPartitions(it => {
                    //获取redis连接
                    val client = RedisUtil.getClient

                    import scala.collection.JavaConversions._
                    //如果返回一个，则放入到集合中，如果返回为空，则返回一个空集合
                    val result = it.flatMap({
                        case (orderId, (Some(orderInfo: OrderInfo), Some(orderDetail: OrderDetail))) =>
                            println("Some", "Some")
                            //order_info 和 order_detail 都有
                            //（1）写入到redis缓冲区
                            cacheOrderInfo(client, orderInfo)
                            //（2）把orderInfo和orderDetail封装在一个样例类SaleDetail中
                            val first = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                            //（3）先从缓冲区读取其余可能之前遗留的orderDetail信息
                            val keys = client.keys(s"order_detail:${orderId}:*").toList
                            //读取到的orderDetail可能会有多个
                            first::keys.map(key => {
                                val orderDetailString = client.get(key)
                                SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(JSON.parseObject(orderDetailString, classOf[OrderDetail]))
                            })


                        case (orderId, (None, Some(orderDetail: OrderDetail))) =>
                            println("None", "Some")
                            //order_info 没有， order_detail 有
                            //(1)根据order_detail中的orderId向缓存中读取对应的orderInfo信息
                            val orderInfoString = client.get("order_info:" + orderId)

                            //(2)读取之后，可能读到order_info也可能读不到
                            if (StringUtils.isNotBlank(orderInfoString)) {
                                val orderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
                                SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)::Nil
                            } else {
                                //读不到则将order_detail写入缓存中
                                cacheOrderDetail(client, orderDetail)
                                Nil
                            }

                        case (orderId, (Some(orderInfo: OrderInfo), None)) =>
                            println("Some", "None")
                            //order_info有， order_detail 没有
                            //(3)orderInfo要写入缓存（考虑到对应的orderDetail有多个，可能还在延迟中）
                            cacheOrderInfo(client, orderInfo)
                            //(1)根据orderId在缓存中读取对应的orderDetail信息
                            //todo:改造为hash结构存储

                            val keys = client.keys(s"order_detail:${orderId}:*").toList
                            //(2)读取到的orderDetail可能会有多个
                            keys.map(key => {
                                val orderDetailString = client.get(key)
                                SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(JSON.parseObject(orderDetailString, classOf[OrderDetail]))
                            })
                    })

                    client.close()
                    result
                })
        stream
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
        val ssc = new StreamingContext(conf, Seconds(3))

        //1，读取kafka中的两个topic(order_info、order_detail)，得到两个流
        val orderInfoStream = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)
                .map(s => {
                    val orderInfo = JSON.parseObject(s, classOf[OrderInfo])
                    //join必须是kv形式的，需要把k作为连接条件进行join
                    (orderInfo.id, orderInfo)
                })

        val orderDetailStream = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER_DETAIL)
                .map(s => {
                    val orderDetail = JSON.parseObject(s, classOf[OrderDetail])
                    //join必须是kv形式的，需要把k作为连接条件进行join
                    (orderDetail.order_id, orderDetail)
                })

        //2，对两个流进行封装，使用full join保证两边数据都不丢
        val saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)
        saleDetailStream.print(1000)
        //3，对两个流进行join合并

        //4，根据用户的id反查mysql中的user_info表，得到用户的生日和性别等信息

        //5，把销售详情写入到es

        //测试
/*        orderInfoStream.print(100)
        orderDetailStream.print(100)*/

        ssc.start()
        ssc.awaitTermination()
    }

}
