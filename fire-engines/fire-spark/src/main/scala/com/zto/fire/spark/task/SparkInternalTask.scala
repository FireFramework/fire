package com.zto.fire.spark.task

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.zto.fire._
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.DatasourceManager
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


  /*@Scheduled(fixedInterval = 10000, scope = "driver", initialDelay = 30000L, concurrent = false)
  def showException: Unit = {
    val queue = this.baseSpark.acc.getLog
    queue.foreach(log => println(log))
  }*/

  /**
   * 数据源收集任务，收集driver与executor用到的数据源信息
   */
  @Scheduled(fixedInterval = 60000L, scope = "all", initialDelay = 60000L, concurrent = false, repeatCount = 30)
  def datasource: Unit = {
    if (FireFrameworkConf.restEnable) {
      val datasourceMap = DatasourceManager.get
      if (datasourceMap.nonEmpty) {
        val json = JSON.toJSONString(datasourceMap, new SerializeConfig(true))
        this.restInvoke("/system/collectDatasource", json)
      }
    }
  }
}
