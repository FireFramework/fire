package com.zto.fire.demo

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

import scala.collection.JavaConversions

object Test extends BaseSparkStreaming {

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    this.log("add count driver")
    val rdd = this.sc.parallelize(1 to 1010, 5)
    rdd.foreach(i => {
      this.mark
      this.log("add count")
    })

    JavaConversions.asScalaIterator(this.acc.getLog.iterator()).foreach(println)
    println("size==>" + this.acc.getLog.size())
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init(100, false)
  }
}
