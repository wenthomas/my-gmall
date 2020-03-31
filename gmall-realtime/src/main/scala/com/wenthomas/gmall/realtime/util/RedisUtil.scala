package com.wenthomas.gmall.realtime.util

import redis.clients.jedis.Jedis


/**
 * @author Verno
 * @create 2020-03-30 14:18 
 */
object RedisUtil {

    private val host: String = PropertiesUtil.getProperty("redis.host")
    private val port = PropertiesUtil.getProperty("redis.port").toInt

    def getClient = {
        val client = new Jedis(host, port, 60 * 1000)
        client.connect()
        client
    }

}
