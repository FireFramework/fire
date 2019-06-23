package com.zto.fire.core

import com.zto.fire.common.util.GlobalConstants
import org.apache.spark.SparkConf

/**
  * 实时平台Spark通用父类
  * Created by ChengLong on 2018-03-28.
  */
class BaseSparkCore extends BaseSpark {

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param conf
    * Spark配置信息
    */
  override def init(conf: SparkConf = null, args: Array[String] = null): Unit = {
    super.init(conf, args)
    this.process
  }


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
        .set("spark.ui.killEnabled", "false")
        .set("spark.port.maxRetries", "200")
        .set("spark.default.parallelism", "1000")
        .set("spark.sql.broadcastTimeout", "3000")
        .set("spark.storage.memoryFraction", "0.4")
        .set("spark.ui.timeline.tasks.maximum", "300")
        .set("spark.sql.parquet.writeLegacyFormat", "true")
        .set("spark.scheduler.listenerbus.eventqueue.size", "130000")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("hive.metastore.uris", GlobalConstants.HiveConf.getMetastoreUrl)
    } else {
      conf
    }
  }

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {}
}
