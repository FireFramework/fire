package com.zto.fire.common.conf

/**
 * 预设状态
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:06
 */
class StatusConfiguration extends Enumeration {
  val SUCCESS = "SUCCESS"
  val FAILED = "FAILED"
  val ERROR = "ERROR"
  val FINISHED = "FINISHED"
  val RUNNING = "RUNNING"
  val UNKNOWN = "UNKNOWN"
}
