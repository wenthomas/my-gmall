package com.wenthomas.gmall.realtime.bean

/**
 * @author Verno
 * @create 2020-04-02 10:59 
 */
case class AlertInfo(mid: String,
                     uids: java.util.HashSet[String],
                     itemIds: java.util.HashSet[String],
                     events: java.util.List[String],
                     ts: Long)
