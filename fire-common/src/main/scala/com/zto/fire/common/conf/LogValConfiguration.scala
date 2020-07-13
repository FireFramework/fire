package com.zto.fire.common.conf

/**
 * log相关常量
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:04
 */
class LogValConfiguration extends Enumeration {
  // log info级别开始
  val logInfoSplitStart = "--->[ "
  // log info级别结束
  val logInfoSplitEnd = " ]<---"
  // log error级别开始
  val logErrorSplitStart = "===>[ "
  // log error级别结束
  val logErrorSplitEnd = " ]<==="
  val logStart = "<================================>"
  val logEnd = "<================================>\n"
}
