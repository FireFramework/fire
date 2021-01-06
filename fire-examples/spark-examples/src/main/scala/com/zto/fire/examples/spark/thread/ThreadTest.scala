package com.zto.fire.examples.spark.thread

import com.zto.fire._
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.spark.BaseSparkStreaming

/**
  * 在driver中启用线程池的示例
  * 1. 开启子线程执行一个任务
  * 2. 开启子线程执行周期性任务
  */
object ThreadTest extends BaseSparkStreaming {

  def main(args: Array[String]): Unit = {
    // 第二个参数为true表示开启checkPoint机制
    this.init(10L, false)
  }

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    // 第一次执行时延迟两分钟，每隔1分钟执行一次showSchema函数
    this.runAsSchedule(this.showSchema, 1, 1)
    // 以子线程方式执行print方法中的逻辑
    this.runAsThread(this.print)

    val dstream = this.fire.createKafkaDirectStream()
    dstream.foreachRDD(rdd => {
      println("count--> " + rdd.count())
    })

    this.fire.start
  }

  /**
    * 以子线程方式执行一次
    */
  def print: Unit = {
    println("==========子线程执行===========")
  }

  /**
    * 查看表结构信息
    */
  def showSchema: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}--------------> atFixRate <----------------")
    this.fire.sql("use tmp")
    this.fire.sql("show tables").show(false)
  }
}
