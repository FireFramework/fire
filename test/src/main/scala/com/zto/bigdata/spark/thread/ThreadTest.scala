package com.zto.bigdata.spark.thread

import com.zto.bigdata.spark.common.core.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.{GlobalConstants, SparkUtils}
import org.apache.spark.storage.StorageLevel

/**
  * 在driver中启用线程池的示例
  * 1. 开启子线程执行一个任务
  * 2. 开启子线程执行周期性任务
  */
object ThreadTest extends BaseSparkStreaming {
  val topics = SparkUtils.topicSplit("thrall2, thrall3, thrall4, thrall5, thrall6, thrall7, thrall8, thrall9")
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"

  def main(args: Array[String]): Unit = {
    this.init(30L, false)

    // 第一次执行时延迟两分钟，每隔1分钟执行一次printCount函数
    this.runAsSchedule(this.printCount, 2, 1)
    // 以子线程方式执行kafka函数
    this.runAsThread(this.kafka)
  }

  /**
    * 接入kafka数据
    */
  def kafka: Unit = {
    val dstream = this.ssc.createDirectStream(this.kafkaParams(this.appName + "2", this.brokers, GlobalConstants.KafkaConf.offsetLargest, false), this.topics, StorageLevel.NONE)
    dstream.foreachRDD((rdd, time) => {
      println("count--> " + rdd.count())
    })

    this.ssc.startAwaitTermination()
  }


  /**
    * 统计表中的记录数
    */
  def printCount: Unit = {
    println("--------------> atFixRate <----------------")
    spark.sql("select count(1) from tmp.test_senda").show(false)
  }
}
