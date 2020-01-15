package com.zto.fire.core

import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.{GlobalConstants, SystemInfoUtils}
import org.apache.spark.SparkConf

/**
  * Structured Streaming通用父类
  * Created by ChengLong on 2019-03-11.
  */
class BaseStructuredStreaming extends BaseSpark {
  override val jobType = JobType.SPARK_STRUCTURED_STREAMING

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param conf
    * Spark配置信息
    * @param args main方法参数
    */
  override def init(conf: Any = null, args: Array[String] = null): Unit = {
    super.init(conf, args)
    // 添加时间监听器
    this.spark.streams.addListener(new BaseStreamingQueryListener)
    if (SystemInfoUtils.isLinux) this.restfulRegister.startRestServer
    this.process
  }

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {}

  /**
    * 构建或合并SparkConf
    *
    * @param conf
    * 在conf基础上构建
    * @return
    * 合并后的SparkConf对象
    */
  override def buildConf(conf: SparkConf): SparkConf = {
    if (conf == null) {
      new SparkConf()
        .setAppName(this.appName)
        .set("spark.port.maxRetries", "200")
        .set("spark.ui.killEnabled", "false")
        .set("spark.default.parallelism", "1000")
        .set("spark.sql.broadcastTimeout", "3000")
        .set("spark.storage.memoryFraction", "0.4")
        .set("spark.ui.timeline.tasks.maximum", "300")
        .set("spark.scheduler.listenerbus.eventqueue.size", "130000")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    } else {
      conf
    }
  }

}
