package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.common.util.PropUtils
import com.zto.fire.spark.BaseSparkStreaming


/**
 * 基于Fire进行Spark Streaming开发
 */
object Test extends BaseSparkStreaming {
  /*@Scheduled(fixedInterval = 10000, scope = "executor", initialDelay = 30000L)
  def collectException: Unit = {

    println("----collectException")
  }

  @Scheduled(fixedInterval = 10000, scope = "driver", initialDelay = 30000L)
  def showException: Unit = {
    /*val queue = this.acc.getLog
    queue.foreach(log => println(log))
    println("----showException")*/
    println("累加值：" + this.acc.getCounter)
  }*/
  override def process: Unit = {
    logger.error("driver打印：" + PropUtils.getString("spark.fire.hello"))
    (1 to 1000).foreach(count => {
      this.fire.createRDD(1 to 1000, 10).foreachPartition(it => {
        logger.error("executor打印：" + PropUtils.getString("spark.fire.hello"))
      })
      Thread.sleep(1000)
    })
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
