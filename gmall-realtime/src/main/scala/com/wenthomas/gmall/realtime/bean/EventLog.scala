package com.wenthomas.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author Verno
 * @create 2020-04-02 10:59 
 */
case class EventLog(mid: String,
                    uid: String,
                    appId: String,
                    area: String,
                    os: String,
                    logType: String,
                    eventId: String,
                    pageId: String,
                    nextPageId: String,
                    itemId: String,
                    ts: Long,
                    var logDate: String = null,
                    var logHour: String = null) {
    val d = new Date(ts)
    logDate = new SimpleDateFormat("yyyy-MM-dd").format(d)
    logHour = new SimpleDateFormat("HH").format(d)
}
