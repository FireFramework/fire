package com.zto.fire.common.conf

/**
 * cron表达式相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:00
 */
private[fire] object FireCronConf {
  lazy val HOUR = "hour"
  lazy val DAY = "day"
  lazy val WEEK = "week"
  lazy val MONTH = "month"
  lazy val YEAR = "year"
  lazy val MINUTE = "minute"
  lazy val SECOND = "second"
  lazy val enumSet = Set(HOUR, DAY, WEEK, MONTH, YEAR, MINUTE, SECOND)
}