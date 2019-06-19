package com.zto.bigdata.spark.common.ext

import com.zto.bigdata.spark.common.ext.core._
import com.zto.bigdata.spark.common.ext.module.LoggerExt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.slf4j.Logger

import scala.reflect._

/**
  * Spark扩展工具类，利用隐式转换对已有的类追加自定义函数
  * Created by ChengLong on 2017-02-07.
  */
object SparkExt {

  /**
    * SparkContext扩展
    *
    * @param spark
    * sparkSession对象
    */
  implicit class SparkSessionExtBridge(spark: SparkSession) extends SparkSessionExt(spark) {

  }

  /**
    * SparkContext扩展
    *
    * @param sc
    * SparkContext对象
    */
  implicit class SparkContextExtBridge(sc: SparkContext) extends SparkContextExt(sc) {

  }


  /**
    * RDD相关的扩展
    *
    * @param rdd
    * rdd
    */
  implicit class RDDExtBridge[T: ClassTag](rdd: RDD[T]) extends RDDExt[T](rdd) {

  }

  /**
    * SparkConf扩展
    *
    * @param sparkConf
    * sparkConf对象
    */
  implicit class SparkConfExtBridge(sparkConf: SparkConf) extends SparkConfExt(sparkConf) {

  }

  /**
    * SQLContext与HiveContext扩展
    *
    * @param sqlContext
    * sqlContext对象
    */
  implicit class SQLContextExtBridge(sqlContext: SQLContext) extends SQLContextExt(sqlContext) {

  }

  /**
    * DataFrame扩展
    *
    * @param dataFrame
    * dataFrame实例
    */
  implicit class DataFrameExtBridge(dataFrame: DataFrame) extends DataFrameExt(dataFrame) {

  }

  /**
    * Dataset扩展
    *
    * @param dataset
    * dataset对象
    */
  implicit class DatasetExtBridge[T: ClassTag](dataset: Dataset[T]) extends DatasetExt[T](dataset) {

  }

  /**
    * StreamingContext扩展
    *
    * @param ssc
    * StreamingContext对象
    */
  implicit class StreamingContextExtBridge(ssc: StreamingContext) extends StreamingContextExt(ssc){

  }


  /**
    * DStream扩展
    *
    * @param stream
    * stream对象
    */
  implicit class DStreamExtBridge[T: ClassTag](stream: DStream[T]) extends DStreamExt[T](stream) {

  }

  /**
    * 日志扩展
    *
    * @param logger
    * 日志记录器
    */
  implicit class LoggerExtBridge(logger: Logging) extends LoggerExt(logger) {

  }
}