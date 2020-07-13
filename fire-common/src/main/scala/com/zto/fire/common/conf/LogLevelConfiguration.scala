package com.zto.fire.common.conf

/**
 * 日志的级别
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:05
 */
class LogLevelConfiguration extends Enumeration {
  val OFF = "OFF"
  val FATAL = "FATAL"
  val ERROR = "ERROR"
  val WARN = "WARN"
  val INFO = "INFO"
  val DEBUG = "DEBUG"
  val TRACE = "TRACE"
  val ALL = "ALL"
}