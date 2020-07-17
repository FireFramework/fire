package com.zto.fire.common.conf

/**
 * 日志的级别
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:05
 */
private[fire] object FireLogLevelConf {
  lazy val OFF = "OFF"
  lazy val FATAL = "FATAL"
  lazy val ERROR = "ERROR"
  lazy val WARN = "WARN"
  lazy val INFO = "INFO"
  lazy val DEBUG = "DEBUG"
  lazy val TRACE = "TRACE"
  lazy val ALL = "ALL"
}