package com.zto.bigdata.spark.common.ext.core

import java.sql.DriverManager

import com.zto.bigdata.spark.common.acc.{MultiAccumulators, MultiDateTimeAccumulators}
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.ext.module.HBaseContextExt
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkContext}

import scala.reflect._

/**
  * SparkContext扩展
  *
  * @param sc
  * SparkContext对象
  * @author ChengLong 2019-5-18 10:53:56
  */
class SparkContextExt(sc: SparkContext) {
  // 获取单例的HBaseContext对象
  private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(sc)

  /**
    * 根据多个key创建一个含有多个值的自定义多值累加器
    */
  def multiAccumulators(): Accumulator[collection.mutable.LinkedHashMap[String, Long]] = {
    val map = scala.collection.mutable.LinkedHashMap[String, Long]()
    this.sc.accumulator(map)(MultiAccumulators)
  }

  /**
    * 根据多个key创建一个含有多个值多个时间的自定义多值累加器
    */
  def multiDateTimeAccumulators: Accumulator[collection.mutable.Map[String, Long]] = {
    val map = scala.collection.mutable.Map[String, Long]()
    this.sc.accumulator(map)(MultiDateTimeAccumulators)
  }

  /**
    * 根据运行模式创建SQLContext或HiveContext
    *
    * @return
    */
  def createSQLContext: SQLContext = {
    if (GlobalConstants.isCluster) {
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
  def setLogLevel2: SparkContext = {
    val logLevel = if (StringUtils.isNotBlank(GlobalConstants.SparkConf.logLevel)) GlobalConstants.SparkConf.logLevel else "DEBUG"
    sc.setLogLevel(logLevel)
    sc
  }

  /**
    * 从关系型数据库中load数据后组装为RDD[bean]
    *
    * @param sql
    * sql语句
    * @param day1
    * 起止时间
    * @param day2
    * 结束时间
    * @param numPartitions
    * 分区数
    * @return
    * RDD
    */
  def loadDBToBean[T: ClassManifest](sql: String, day1: String, day2: String, clazz: Class[T], numPartitions: Int = GlobalConstants.SparkConf.parallelism): RDD[T] = {
    val lowerBound = DateFormatUtils.formatDateTime(day1).getTime / 1000
    val upperBound = DateFormatUtils.formatDateTime(day2).getTime / 1000
    new JdbcRDD(
      sc,
      () => {
        Class.forName(GlobalConstants.JdbcConf.driverClass).newInstance()
        DriverManager.getConnection(GlobalConstants.JdbcConf.url, GlobalConstants.JdbcConf.user, GlobalConstants.JdbcConf.password)
      },
      sql,
      lowerBound, upperBound, numPartitions,
      row => SparkUtils.dbRow2Bean(row, clazz))
  }

  /**
    * 定义多个Long类型累加器
    *
    * @return
    * map
    */
  def defineLongAccumulators(accNames: String*): Map[String, Accumulator[Long]] = {
    var accMap = Map[String, Accumulator[Long]]()
    accNames.foreach(accName => {
      accMap += (accName -> sc.accumulator[Long](0L))
    })
    accMap
  }

}