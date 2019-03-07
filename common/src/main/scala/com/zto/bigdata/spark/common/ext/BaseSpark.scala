package com.zto.bigdata.spark.common.ext

import com.zto.bigdata.spark.common.util.{GlobalConstants, SparkUtils}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark通用父类
  * Created by ChengLong on 2018-03-06.
  */
trait BaseSpark extends SparkListener with Serializable {
  var conf: SparkConf = _
  var spark: SparkSession = _
  var sc: SparkContext = _
  var hiveContext: SQLContext = _
  var sqlContext: SQLContext = _
  var kuduContext: KuduContextExt = _
  var hbaseContext: HBaseContextExt = _
  val startTime = SparkUtils.currentTime
  var appName = this.getClass.getSimpleName.replace("$", "")

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param appName
    * job名称
    * @param conf
    * Spark配置信息
    */
  def init(beanDir: String = "", appName: String = "", conf: SparkConf = null): Unit

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  def process: Unit

  /**
    * 打印总耗时
    *
    * @param applicationEnd
    * 整个job执行结束后执行
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (this.hiveContext != null) this.hiveContext.clearCache
    GlobalConstants.PrintModule.END_TIME_COST(this.startTime)
  }
}