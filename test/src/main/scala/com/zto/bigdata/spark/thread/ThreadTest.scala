package com.zto.bigdata.spark.thread

import com.zto.bigdata.spark.common.core.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._

/**
  * 在driver中启用线程池的示例
  * 1. 开启子线程执行一个任务
  * 2. 开启子线程执行周期性任务
  */
object ThreadTest extends BaseSparkStreaming {

  def main(args: Array[String]): Unit = {
    this.init(10L, false)

    // 第一次执行时延迟两分钟，每隔1分钟执行一次printCount函数
    this.runAsSchedule(this.showSchema, 1, 1)
    // 以子线程方式执行kafka函数
    this.runAsThread(this.kafka)
  }


  /**
    * 接入kafka数据
    */
  def kafka: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD(rdd => {
      println("count--> " + rdd.count())
    })

    this.ssc.startAwaitTermination()
  }


  /**
    * 统计表中的记录数
    */
  def showSchema: Unit = {
    println("--------------> atFixRate <----------------")
    spark.sql("desc ods.gd_scan_send_new").show(false)
  }
}
