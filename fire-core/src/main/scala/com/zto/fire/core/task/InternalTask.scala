package com.zto.fire.core.task

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSpark
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.util.SparkUtils
import org.apache.spark.Logging

/**
 * 定时任务调度器，用于定时执行fire框架内部指定的任务
 *
 * @author ChengLong 2019年11月5日 10:11:31
 */
private[fire] class InternalTask(baseSpark: BaseSpark) extends Logging with Serializable {

  /**
   * 1s执行一次，仅在executor端执行，默认运行并行执行该方法
   * 比如：方法执行时间超过1s，将会有多个实例同时执行
   * initialDelay=0表示立即执行
   * repeatCount=3表示只执行3次
   */
  @Scheduled(fixedInterval = 10000, scope = "executor", initialDelay = 0L, startAt = "2019-11-05 12:00:00")
  def test2: Unit = {
    this.log("executorId= " + SparkUtils.getExecutorId + "====方法 test2() 每10秒执行====" + DateFormatUtils.formatCurrentDateTime())
  }

  /**
   * 1s执行一次，仅在executor端执行，默认运行并行执行该方法
   * 比如：方法执行时间超过1s，将会有多个实例同时执行
   * initialDelay=0表示立即执行
   * repeatCount=3表示只执行3次
   */
  @Scheduled(cron = "0/30 * * * * ?", repeatCount = 100)
  def test3: Unit = {
    this.log("executorId=" + SparkUtils.getExecutorId + " ====方法 test3() 每30秒执行====" + DateFormatUtils.formatCurrentDateTime())
  }
}
