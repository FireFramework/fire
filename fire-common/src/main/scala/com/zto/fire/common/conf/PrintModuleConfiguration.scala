package com.zto.fire.common.conf

import com.zto.fire.common.util.{DateFormatUtils, GlobalConstants}

/**
 * 打印模块枚举
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:03
 */
class PrintModuleConfiguration extends Enumeration {
  // 打印多值累加器开始
  def MULTI_ACC_START: Unit = println(s"[${GlobalConstants.PS1.PINK}${DateFormatUtils.formatCurrentDateTime()}${GlobalConstants.PS1.DEFAULT}]--- ${GlobalConstants.PS1.GREEN}MultiAccumulators Start ... ${GlobalConstants.PS1.DEFAULT}---------------------------------------------")

  // 打印多值多日期累加器开始
  def MULTI_ACC_DATE_TIME_START: Unit = println(s"[${GlobalConstants.PS1.PINK}${DateFormatUtils.formatCurrentDateTime()}${GlobalConstants.PS1.DEFAULT}]--- ${GlobalConstants.PS1.GREEN}MultiDateTimeAccumulators Start ... ${GlobalConstants.PS1.DEFAULT}---------------------------------------------")

  // 打印多值累加器结束
  def MULTI_ACC_END: Unit = println(s"------------------------ ${GlobalConstants.PS1.GREEN}MultiAccumulators End   ... ${GlobalConstants.PS1.DEFAULT}---------------------------------------------\n\n")

  // 打印多值多日期累加器结束
  def MULTI_ACC_DATE_TIME_END: Unit = println(s"------------------------ ${GlobalConstants.PS1.GREEN}MultiDateTimeAccumulators End   ... ${GlobalConstants.PS1.DEFAULT}---------------------------------------------\n\n")

  // 打印多值累加器清零
  def MULTI_ACC_CLEAR: Unit = println(s"------------------------ ${GlobalConstants.PS1.RED}*********** 清零累加器 ***********${GlobalConstants.PS1.DEFAULT}  ---------------------------------------------")

  // 打印多值累加器中的值
  def MULTI_ACC_VALUE(t: (String, Long)): Unit = println(s"${t._1} : ${GlobalConstants.PS1.YELLOW}${t._2}${GlobalConstants.PS1.DEFAULT}")

  // 总耗时打印
  def END_TIME_COST(startTime: Long): Unit = println(s"总耗时：${GlobalConstants.PS1.RED}${DateFormatUtils.runTime(startTime)}${GlobalConstants.PS1.DEFAULT} The end...${GlobalConstants.PS1.DEFAULT}")

  // 实时相关
  def REAL_TIME_PROCESS_METHOD: String = s"${GlobalConstants.PS1.RED}子类必须通过覆写process()方法实现具体逻辑${GlobalConstants.PS1.DEFAULT}"
}