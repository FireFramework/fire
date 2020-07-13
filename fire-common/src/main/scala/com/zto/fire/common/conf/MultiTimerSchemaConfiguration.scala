package com.zto.fire.common.conf

/**
 * 用于定义累加日期的维度
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:06
 */
class MultiTimerSchemaConfiguration extends Enumeration {
  val SEC = "yyyy-MM-dd HH:mm:ss"
  val MIN = "yyyy-MM-dd HH:mm:00"
  val HOUR = "yyyy-MM-dd HH:00:00"
  val DAY = "yyyy-MM-dd 00:00:00"

  // 其他用于自定义日期格式
  def other(schema: String): String = schema
}