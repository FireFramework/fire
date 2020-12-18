package com.zto.fire.spark.ext.core

import com.zto.fire.common.conf.FireSparkConf
import com.zto.fire.spark.ext.SparkExt._
import com.zto.fire.spark.util.SparkUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkContext}

/**
  * SparkContext扩展
  *
  * @param sc
  * SparkContext对象
  * @author ChengLong 2019-5-18 10:53:56
  */
class SparkContextExt(sc: SparkContext) {
  /**
    * 根据运行模式创建SQLContext或HiveContext
    *
    * @return
    */
  @deprecated("use sparkSession.sqlContext", "v1.1.1")
  def createSQLContext: SQLContext = {
    if (SparkUtils.isCluster) {
      new HiveContext(sc)
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("hive.exec.max.dynamic.partitions", "1000")
        .set("hive.exec.max.dynamic.partitions.pernode", "1000")
        .set("hive.exec.compress.output", "true").set("mapred.output.compress", "true")
        .set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        .set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec")
    } else {
      new SQLContext(sc)
    }
  }

  /**
    * 设置日志级别
    *
    * @return
    */
  @deprecated
  def setLogLevel2: SparkContext = {
    val logLevel = if (StringUtils.isNotBlank(FireSparkConf.logLevel)) FireSparkConf.logLevel else "DEBUG"
    sc.setLogLevel(logLevel)
    sc
  }

  /**
    * 定义多个Long类型累加器
    *
    * @return
    * map
    */
  @deprecated
  def defineLongAccumulators(accNames: String*): Map[String, Accumulator[Long]] = {
    var accMap = Map[String, Accumulator[Long]]()
    accNames.foreach(accName => {
      accMap += (accName -> sc.accumulator[Long](0L))
    })
    accMap
  }

}