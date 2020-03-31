package com.wenthomas.gmall.realtime.util

import java.io.InputStream
import java.util.Properties

/**
 * @author Verno
 * @create 2020-03-30 11:23 
 */
object PropertiesUtil {

    //流
    private val inputStream: InputStream = PropertiesUtil.getClass.getClassLoader.getResourceAsStream("config.properties")

    private val props = new Properties()
    props.load(inputStream)

    def getProperty(propertyName: String): String = {
        props.getProperty(propertyName)
    }

    /**
     * 测试
     * @param args
     */
    def main(args: Array[String]): Unit = {
        println(getProperty("kafka.broker.list"))
    }

}
