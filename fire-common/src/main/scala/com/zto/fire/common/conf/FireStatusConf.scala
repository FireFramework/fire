package com.zto.fire.common.conf

/**
 * 预设状态
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:06
 */
private[fire] object FireStatusConf {
  lazy val SUCCESS = "SUCCESS"
  lazy val FAILED = "FAILED"
  lazy val ERROR = "ERROR"
  lazy val FINISHED = "FINISHED"
  lazy val RUNNING = "RUNNING"
  lazy val UNKNOWN = "UNKNOWN"
}