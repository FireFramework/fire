package com.zto.fire.core.task

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.acc.AccumulatorManager
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.bean.runtime.RuntimeInfo
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
   * 定时采集运行时的jvm、gc、thread、cpu、memory、disk等信息
   * 并将采集到的数据存放到EnvironmentAccumulator中
   */
  @Scheduled(fixedInterval = 5000, scope = "all", initialDelay = 0L, concurrent = false)
  def collectEnvironmentInfo: Unit = {
    AccumulatorManager.addEnv(JSON.toJSONString(RuntimeInfo.getRuntimeInfo, SerializerFeature.NotWriteRootClassName))
  }

}
