package com.zto.fire.demo

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._

import scala.collection.JavaConversions

object Test extends BaseSparkCore {

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    this.acc.addCounter(1)
    this.acc.addCounter(1)
    this.log("add count driver")
    val rdd = this.sc.parallelize(1 to 10, 10)
    rdd.foreach(i => {
      this.mark
      this.acc.addCounter(1)
      Thread.sleep(100)
      this.log("add count")
    })
    println(this.acc.getCounter)
    rdd.foreach(i => {
      this.acc.addCounter(1)
      Thread.sleep(100)
      this.log("add count2")
    })
    println(this.acc.getCounter)

    JavaConversions.mapAsScalaConcurrentMap(this.acc.getLog).foreach(t => println(t.toString()))
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
