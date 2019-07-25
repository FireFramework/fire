package com.zto.bigdata.spark

import com.zto.fire.common.util.AccumulatorUtils
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._

import scala.collection.JavaConversions

object Test extends BaseSparkCore {

  override def process: Unit = {
    val rdd = spark.sparkContext.parallelize(1 to 20, 20)

    rdd.foreach(i => {
      this.mark
      AccumulatorUtils.addCountValue(1)
      Thread.sleep(1000)
      this.log("hello world")
    })

    JavaConversions.mapAsScalaMap(this.logAccumulator.value).foreach(t => {
      println(t._1 + " value=" + t._2)
    })
    println("执行完成: count=" + this.count.value)
  }

  def main(args: Array[String]): Unit = {
    this.init()

    Thread.currentThread().join()
  }

}
