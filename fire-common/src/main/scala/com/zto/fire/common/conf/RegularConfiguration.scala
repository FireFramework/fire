package com.zto.fire.common.conf

/**
 * 预定义的一些正则表达式
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:04
 */
class RegularConfiguration extends Enumeration {
  val DOUBLE_DATE_PATTERN = "\\d+_\\d+".r
  // 匹配形如2018051912的时间，前面有_
  val DATE_TIME_PATTERN = "_\\d{10}$".r
  // 匹配一个以上的数字
  val MULTI_NUMBER_PATTERN = "_\\d+$".r
  // 只能包含字母和下划线
  val NO_NUMBER = "^[A-Za-z_]+$".r
  // 匹配applicationId，兼容后缀为4位或5位数字
  val APPLICATION_ID = "application_\\d{13}_\\d{4,5}".r
}
