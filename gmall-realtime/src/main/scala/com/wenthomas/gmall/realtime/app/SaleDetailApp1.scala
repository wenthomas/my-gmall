package com.wenthomas.gmall.realtime.app

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.wenthomas.gmall.common.{Constant, ESUtil}
import com.wenthomas.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.wenthomas.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

/**
 * @author Verno
 * @create 2020-04-08 9:18 
 */
object SaleDetailApp1 {

    /**
     * 写入redis(string数据类型方案)
     * @param client
     * @param key
     * @param value
     * @return
     */
    def saveToRedis(client: Jedis, key: String, value: AnyRef) = {
        //样例类不能用fastJson来解析，使用Serialization.write
        import org.json4s.DefaultFormats
        val jsonString = Serialization.write(value)(DefaultFormats)
        //key需要设置过期时间
        client.setex(key, 60 * 30, jsonString)
    }

    def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo)= {
        val key = "order_info:" + orderInfo.id
        saveToRedis(client, key, orderInfo)
    }

    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail) = {
        val key = s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
        saveToRedis(client, key, orderDetail)
    }

    /**
     * 写入redis（Hash数据类型方案）
     * 优点： key数量固定为order_info和order_detail两个
     * 缺点：存取相较于string数据类型会麻烦一点
     * @param client
     * @param key       redis key
     * @param field     hash的key
     * @param value     hash的value
     * @return
     */
    def saveToRedisHash(client: Jedis, key: String, field: String, value: AnyRef) = {
        //样例类不能用fastJson来解析，使用Serialization.write
        import org.json4s.DefaultFormats
        val jsonString = Serialization.write(value)(DefaultFormats)
        val map: util.Map[String, String]  = new util.HashMap[String, String]()
        map.put(field, jsonString)
        //hash存储的是java map
        client.hmset(key, map)
    }

    def cacheOrderInfoHash(client: Jedis, orderInfo: OrderInfo)= {
        val key = "order_info"
        val field = orderInfo.id
        saveToRedisHash(client, key, field, orderInfo)
    }

    def cacheOrderDetailHash(client: Jedis, orderDetail: OrderDetail) = {
        val key = "order_detail"
        val field = s"${orderDetail.order_id}:${orderDetail.id}"
        saveToRedisHash(client, key, field, orderDetail)
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
                        case (orderId, (Some(orderInfo: OrderInfo), opt)) =>
                            println("Some", "Option")
                            //情景一：order_info 有， order_detail 有或没有
                            //（1）写入到redis缓冲区
                            cacheOrderInfo(client, orderInfo)
                            //                            cacheOrderInfoHash(client, orderInfo)

                            //（3）先从缓冲区读取其余可能之前遗留的orderDetail信息
                            val keys = client.keys(s"order_detail:${orderId}:*").toList
                            //读取到的orderDetail可能会有多个
                            keys.map(key => {
                                val orderDetailString = client.get(key)
                                //读完缓存后删除order_detail端的缓存
                                client.del(key)
                                SaleDetail()
                                        .mergeOrderInfo(orderInfo)
                                        .mergeOrderDetail(JSON.parseObject(orderDetailString, classOf[OrderDetail]))
                            }) ::: (opt match {
                                case Some(orderDetail) =>
                                    SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)::Nil
                                case None =>
                                    Nil
                            })


                        /*                            val fields = client.hkeys("order_detail").toList
                                                    //获取到orderId对应的所有orderDetail的id值
                                                    val orderDetails = fields.filter(_.split(":")(0) == orderId)
                                                    first::orderDetails.map(field => {
                                                        val orderDetailString = client.hget("order_detail", field)
                                                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(JSON.parseObject(orderDetailString, classOf[OrderDetail]))
                                                    })*/


                        case (orderId, (None, Some(orderDetail: OrderDetail))) =>
                            println("None", "Some")
                            //情景二：order_info 没有， order_detail 有
                            //(1)根据order_detail中的orderId向缓存中读取对应的orderInfo信息
                            val orderInfoString = client.get("order_info:" + orderId)
                            //                            val orderInfoString = client.hget("order_info", orderId)

                            //(2)读取之后，可能读到order_info也可能读不到
                            if (StringUtils.isNotBlank(orderInfoString)) {
                                val orderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
                                SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)::Nil
                            } else {
                                //读不到则将order_detail写入缓存中
                                cacheOrderDetail(client, orderDetail)
                                //                                cacheOrderDetailHash(client, orderDetail)
                                Nil
                            }
                    })

                    client.close()
                    result
                })
        stream
    }

    /**
     * 反查mysql中的user_info信息，将所需信息封装入saleDetailStream中
     * @param ssc
     * @param saleDetailStream
     */
    def joinUser(ssc: StreamingContext, saleDetailStream: DStream[SaleDetail]) = {
        //jdbc连接属性
        val url = "jdbc:mysql://mydata03:3306/gmall"
        val user = "root"
        val passwd = "123456"
        val props = new Properties()
        props.setProperty("user", user)
        props.setProperty("password", passwd)

        val spark = SparkSession.builder()
                .config(ssc.sparkContext.getConf)
                .getOrCreate()
        import spark.implicits._

        //1，先把mysql数据读取出来，每个3s读一次（为了能读到新添加的user）
        saleDetailStream.transform(saleDetailRDD => {
            //方案一：直接在driver中去把数据读取出来
            val userInfoRDD = spark.read.jdbc(url, Constant.TABLE_USER_INFO, props)
                            .as[UserInfo]
                            .rdd
                            .map(user => (user.id, user))

            //两个RDD进行join操作
            saleDetailRDD.map(saleDetail => (saleDetail.user_id, saleDetail))
                            .join(userInfoRDD)
                            .map({
                                case (userId, (saleDetail, userInfo)) =>
                                    saleDetail.mergeUserInfo(userInfo)
                            })

            //方案二：或者每个分区分别进行join，参考map join的代码
        })
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
        //3，对两个流进行join合并
        var saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)

        //4，根据用户的id反查mysql中的user_info表，得到用户的生日和性别等信息
        saleDetailStream = joinUser(ssc, saleDetailStream)
        saleDetailStream.print(1000)

        //5，把销售详情写入到es
        saleDetailStream.foreachRDD(rdd => {
            //方案一：可以把rdd的所有数据拉到driver，一次性写入
            //优点：一次写入
            //缺点：数据量大时会OOM
            ESUtil.insertBulk(Constant.INDEX_SALE_DETAIL, rdd.collect.toIterator)

            //方案二：每个分区分别写入到ES
/*            rdd.foreachPartition(it => {
                ESUtil.insertBulk(Constant.INDEX_SALE_DETAIL, it)
            })*/
        })



        //测试
        /*        orderInfoStream.print(100)
                orderDetailStream.print(100)*/

        ssc.start()
        ssc.awaitTermination()
    }

}
