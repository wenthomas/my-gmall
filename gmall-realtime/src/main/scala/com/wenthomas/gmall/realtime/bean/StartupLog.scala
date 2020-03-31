package com.wenthomas.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author Verno
 * @create 2020-03-30 23:39 
 */
case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long,
                      var logDate: String,
                      var logHour: String) {
    val d = new Date(ts)
    logDate = new SimpleDateFormat("yyyy-MM-dd").format(d)
    logHour = new SimpleDateFormat("HH").format(d)
}
