package com.zto.fire.common.conf

/**
 * 日期模式类型
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:47
 */
private[fire] object FireDateSchemaConf {
  lazy val yyyy_MM_ddHHmmss = "yyyy-MM-dd HH:mm:ss"
  lazy val yyyyMMdd = "yyyyMMdd"
  lazy val yyyy_MM_dd = "yyyy-MM-dd"
  lazy val yyyyMMddHH = "yyyyMMddHH"

  lazy val SEC = "yyyy-MM-dd HH:mm:ss"
  lazy val MIN = "yyyy-MM-dd HH:mm:00"
  lazy val HOUR = "yyyy-MM-dd HH:00:00"
  lazy val DAY = "yyyy-MM-dd 00:00:00"

  // 其他用于自定义日期格式
  def other(schema: String): String = schema
}