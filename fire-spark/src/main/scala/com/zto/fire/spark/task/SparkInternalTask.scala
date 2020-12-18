package com.zto.fire.spark.task

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.core.task.FireInternalTask
import com.zto.fire.spark.BaseSpark

/**
 * 定时任务调度器，用于定时执行Spark框架内部指定的任务
 *
 * @author ChengLong 2019年11月5日 10:11:31
 */
private[fire] class SparkInternalTask(baseSpark: BaseSpark) extends FireInternalTask(baseSpark) {

  /**
   * 定时采集运行时的jvm、gc、thread、cpu、memory、disk等信息
   * 并将采集到的数据存放到EnvironmentAccumulator中
   */
  @Scheduled(fixedInterval = 60000, scope = "all", initialDelay = 60000L, concurrent = false)
  override def jvmMonitor: Unit = super.jvmMonitor
}
