package com.wenthomas.dw.gmall.canal

import java.net.InetSocketAddress

import com.alibaba.otter.canal.client.CanalConnectors
import com.alibaba.otter.canal.protocol.CanalEntry
import java.util

import com.alibaba.otter.canal.protocol.CanalEntry.RowChange
import com.wenthomas.dw.gmall.util.CanalHandler
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * @author Verno
 * @create 2020-04-01 9:17 
 */
object CanalClient {

    def main(args: Array[String]): Unit = {
        //1，连接到canal服务端
        val address = new InetSocketAddress("mydata03", 11111)
        //todo: canal用户名密码
        //val connector = CanalConnectors.newSingleConnector(address, "example", "root", "123456")
        val connector = CanalConnectors.newSingleConnector(address, "example", "", "")

        println("connector: " + connector.checkValid())

        connector.connect()
        //订阅数据：gmall数据库下所有的表
        connector.subscribe("gmall.*")

        //------------------------------------------------------------------------------------------
        //2，读数据，解析数据
        while (true) {
            //2.1，使用循环的方式持续地从canal服务端读取数据
            //2.2，一次从canal拉取最多100条sql引起的数据的变化
            val message = connector.get(100)
            // 一个entry封装了一条sql引起的变化结果
            //为什么这里会报错找不到List:需要导入import scala.collection.JavaConversions._
            val entriesOption = if (message != null) Some(message.getEntries) else None
            if (entriesOption.isDefined && entriesOption.get.nonEmpty) {
                val entries = entriesOption.get
                for (entry <- entries) {
                    //从每个entry中获取一个StoreValue
                    val storeValue = entry.getStoreValue
                    //把storeValue解析出rowChange
                    val rowChange = RowChange.parseFrom(storeValue)
                    //一个storeValue中有多个RowData,每个RowData表示一行数据的变化
                    val rowDatas = rowChange.getRowDatasList

                    //3，把数据转成json字符串写入到kafka中
                    //解析RowData中的每行每列数据，并发送至kafka
                    CanalHandler.handleData(entry.getHeader.getTableName,
                        rowChange.getEventType,
                        rowDatas)
                }
            } else {
                println("没有拉取到数据，2s之后重新拉取")
                Thread.sleep(2000)
            }
        }
    }
}
