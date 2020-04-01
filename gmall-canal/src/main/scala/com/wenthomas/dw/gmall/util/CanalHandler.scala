package com.wenthomas.dw.gmall.util

import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowData}
import java.util

import com.alibaba.fastjson.JSONObject
import com.wenthomas.gmall.common.Constant

import scala.collection.JavaConversions._

/**
 * @author Verno
 * @create 2020-04-01 11:45 
 */
/**
 * Canal工具类
 */
object CanalHandler {
    //todo:最好try catch并返回是否成功给调用方
    def handleData(tableName: String, eventType: EventType, rowDataList: util.List[RowData]) = {
        //导入import scala.collection.JavaConversions._
        if ("order_info" == tableName && eventType == EventType.INSERT && !rowDataList.isEmpty) {
            //1，rowData表示一行数据，通过他得到每一列，首先遍历每一行的数据
            for (rowData <- rowDataList) {
                val result = new JSONObject()
                //2，得到每一行中，所有列组成的列表
                val columnsList = rowData.getAfterColumnsList
                for (column <- columnsList) {
                    //3，得到列名和列值
                    val key = column.getName
                    val value = column.getValue
                    result.put(key, value)
                    println("rowchange----" + key + ":" + value)
                }
                //4，将每行数据变化写到Kafka
                MyKafkaUtil.send(Constant.TOPIC_ORDER_INFO, result.toJSONString)
            }
        }
    }
}
