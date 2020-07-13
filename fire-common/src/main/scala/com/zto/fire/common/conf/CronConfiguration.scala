package com.zto.fire.common.conf

/**
 * cron表达式相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:00
 */
class CronConfiguration extends Enumeration {
  val HOUR = "hour"
  val DAY = "day"
  val WEEK = "week"
  val MONTH = "month"
  val YEAR = "year"
  val MINUTE = "minute"
  val SECOND = "second"
  val enumSet = Set(HOUR, DAY, WEEK, MONTH, YEAR, MINUTE, SECOND)
}