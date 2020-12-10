package com.zto.fire.common.conf

import com.zto.fire.common.util.DateFormatUtils

/**
 * 打印模块枚举
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:03
 */
private[fire] object FirePrintModuleConf {
  // 打印多值累加器开始
  def multiAccStart: Unit = println(s"[${FirePS1Conf.PINK}${DateFormatUtils.formatCurrentDateTime()}${FirePS1Conf.DEFAULT}]--- ${FirePS1Conf.GREEN}MultiAccumulators Start ... ${FirePS1Conf.DEFAULT}---------------------------------------------")

  // 打印多值多日期累加器开始
  def multiAccDateTimeStart: Unit = println(s"[${FirePS1Conf.PINK}${DateFormatUtils.formatCurrentDateTime()}${FirePS1Conf.DEFAULT}]--- ${FirePS1Conf.GREEN}MultiDateTimeAccumulators Start ... ${FirePS1Conf.DEFAULT}---------------------------------------------")

  // 打印多值累加器结束
  def multiAccEnd: Unit = println(s"------------------------ ${FirePS1Conf.GREEN}MultiAccumulators End   ... ${FirePS1Conf.DEFAULT}---------------------------------------------\n\n")

  // 打印多值多日期累加器结束
  def multiAccDateTimeEnd: Unit = println(s"------------------------ ${FirePS1Conf.GREEN}MultiDateTimeAccumulators End   ... ${FirePS1Conf.DEFAULT}---------------------------------------------\n\n")

  // 打印多值累加器清零
  def multiAccClear: Unit = println(s"------------------------ ${FirePS1Conf.RED}*********** 清零累加器 ***********${FirePS1Conf.DEFAULT}  ---------------------------------------------")

  // 打印多值累加器中的值
  def multiAccValue(t: (String, Long)): Unit = println(s"${t._1} : ${FirePS1Conf.YELLOW}${t._2}${FirePS1Conf.DEFAULT}")

  // 总耗时打印
  def endTimeCost(startTime: Long): Unit = println(s"总耗时：${FirePS1Conf.RED}${DateFormatUtils.runTime(startTime)}${FirePS1Conf.DEFAULT} The end...${FirePS1Conf.DEFAULT}")

  // 实时相关
  def realTimeProcessMethod: String = s"${FirePS1Conf.RED}子类必须通过覆写process()方法实现具体逻辑${FirePS1Conf.DEFAULT}"
}