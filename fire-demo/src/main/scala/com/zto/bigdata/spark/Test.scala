package com.zto.bigdata.spark


import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.BaseSparkCore

object Test extends BaseSparkCore {

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    val start = System.currentTimeMillis()
    val rdd = spark.sparkContext.parallelize(1 to 20, 5)
    this.log("--------------------->1.driver")
    this.log("--------------------->2.driver")

    rdd.foreach(index => {
      this.mark
      Thread.sleep(1000)
      log("日志 1")
      this.mark
      Thread.sleep(1)
      log("日志 2")
      log("日志 3")
    })

    println("耗时：" + (System.currentTimeMillis() - start))
  }

  def main(args: Array[String]): Unit = {
    this.init()

    Thread.currentThread().join()
  }

}
