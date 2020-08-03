package com.zto.fire.common.conf

/**
 * log相关常量
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:05
 */
private[fire] object FireLogValConf {
  // log info级别开始
  lazy val logInfoSplitStart = "--->[ "
  // log info级别结束
  lazy val logInfoSplitEnd = " ]<---"
  // log error级别开始
  lazy val logErrorSplitStart = "===>[ "
  // log error级别结束
  lazy val logErrorSplitEnd = " ]<==="
  lazy val logStart = "<================================>"
  lazy val logEnd = "<================================>\n"
}