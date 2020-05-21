package com.zto.fire.demo.spark.acc

import java.util.concurrent.TimeUnit

import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.util.SparkUtils

import scala.collection.JavaConversions

/**
 * 用于演示与测试Fire框架内置的累加器
 *
 * @author ChengLong 2019年9月10日 09:50:16
 */
object FireAccTest extends BaseSparkStreaming {

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD(rdd => {
      rdd.foreachPartition(t => {
        this.mark
        // 单值累加器
        this.acc.addCounter(1)
        // 多值累加器，根据key的不同分别进行数据的累加
        this.acc.addMultiCounter("multiCounter", 1)
        this.acc.addMultiCounter("partitions", 1)
        // 多时间维度累加器，比多值累加器多了一个时间维度，如：hbaseWriter  2019-09-10 11:00:00  10
        this.acc.addMultiTimer("multiTimer", 1)
        // 日志内容将被日志累加器收集
        this.log("日志累加器：executorId=" + SparkUtils.getExecutorId)
      })
    })

    // 定时打印fire内置累加器中的值
    this.runAsSchedule(this.printAcc, 0, 10, true, TimeUnit.MINUTES)

    this.ssc.startAwaitTermination()
  }

  /**
   * 打印累加器中的值
   */
  def printAcc: Unit = {
    println(s"===============${DateFormatUtils.formatCurrentDateTime()}=============")
    JavaConversions.asScalaSet(this.acc.getMultiTimer.cellSet()).foreach(t => println(s"key：" + t.getRowKey + " 时间：" + t.getColumnKey + " " + t.getValue + "条"))

    println("单值：" + this.acc.getCounter)
    JavaConversions.mapAsScalaConcurrentMap(this.acc.getMultiCounter).foreach(t => {
      println("多值：key=" + t._1 + " value=" + t._2)
    })
    val size = this.acc.getMultiTimer.cellSet().size()

    println(s"======multiTimer.size=${size}==log.size=${this.acc.getLog.size()}======")
  }

  def main(args: Array[String]): Unit = {
    this.init(1, false)
  }
}
