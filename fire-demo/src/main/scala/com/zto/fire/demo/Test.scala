package com.zto.fire.demo

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

object Test extends BaseSparkStreaming {

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    val rdd = this.sc.parallelize(1 to 1010, 100)
    this.mark
    rdd.foreachPartition(i => {
      i.foreach(index => {
        this.acc.addMultiCounter("rdd.count", 1)
      })
      this.acc.addMultiCounter("task.count", 1)
    })
    this.log("add count")

    JavaConversions.mapAsScalaConcurrentMap(this.acc.getMultiCounter).foreach(t => println(t._1 + " " + t._2))
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init(100, false)
  }
}
