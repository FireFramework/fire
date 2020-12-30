package com.zto.fire.spark.ext.core

import com.zto.fire.jdbc.JdbcConnectorBridge
import com.zto.fire.spark.ext.provider._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

/**
 * SparkContext扩展
 *
 * @param spark
 * sparkSession对象
 * @author ChengLong 2019-5-18 10:51:19
 */
private[fire] class SparkSessionExt(spark: SparkSession) extends JdbcConnectorBridge with JdbcSparkProvider
  with HBaseBulkProvider with SqlProvider with HBaseConnectorProvider with HBaseHadoopProvider with KafkaSparkProvider {

  /**
   * 根据给定的集合，创建rdd
   *
   * @param seq
   * seq
   * @param numSlices
   * 分区数
   * @return
   * RDD
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
    this.sc.parallelize(seq, numSlices)
  }
}