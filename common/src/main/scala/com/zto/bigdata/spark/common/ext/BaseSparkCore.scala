package com.zto.bigdata.spark.common.ext

import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 实时平台Spark通用父类
  * Created by ChengLong on 2018-03-28.
  */
class BaseSparkCore extends BaseSpark {

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param appName
    * job名称
    * @param conf
    * Spark配置信息
    */
  override def init(appName: String = "", conf: SparkConf = null): Unit = {
    val tmpAppName = if (StringUtils.isBlank(appName)) this.appName else appName
    if (conf == null) {
      this.conf = new SparkConf()
        .setAppName(tmpAppName)
        .set("spark.port.maxRetries", "200")
        .set("spark.default.parallelism", "1000")
        .set("spark.sql.broadcastTimeout", "3000")
        .set("spark.storage.memoryFraction", "0.4")
        .set("spark.ui.timeline.tasks.maximum", "300")
        .set("spark.scheduler.listenerbus.eventqueue.size", "130000")
        .set("spark.sql.parquet.writeLegacyFormat", "true")
        .set("hive.metastore.uris", GlobalConstants.HiveConf.metaStoreUris)
    } else {
      this.conf = conf
    }
    if (SystemInfoUtils.isWindows) {
      this.spark = SparkSession.builder().config(this.conf).master("local[*]").enableHiveSupport().getOrCreate()
    } else {
      this.spark = SparkSession.builder().config(this.conf).enableHiveSupport().getOrCreateCarbonSession
    }
    this.sc = this.spark.sparkContext
    this.sc.setLogLevel(GlobalConstants.SparkConf.logLevel)
    this.sc.addSparkListener(new BaseSparkListener(this))
    this.hiveContext = this.spark.sqlContext
    this.hiveContext.registerAll()
    this.sqlContext = this.hiveContext
    this.hbaseContext = SingletonFactory.getHBaseContextInstance(sc)
    this.applicationId = SparkUtils.getApplicationId(this.spark)
    this.webUI = SparkUtils.getWebUI(this.spark)
    this.process
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {}
}
