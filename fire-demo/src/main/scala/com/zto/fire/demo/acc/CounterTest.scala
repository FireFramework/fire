package com.zto.fire.demo.acc

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._

import scala.collection.JavaConversions

/**
  * Counter累加器的使用示例
  * 2019-8-11 10:50:17
  */
object CounterTest extends BaseSparkCore {

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    this.acc.addCounter(1)
    val rdd = this.sc.parallelize(1 to 10, 10)

    rdd.foreach(i => {
      this.mark
      // 将值添加到counter累加器中
      this.acc.addCounter(1)
      this.log("add count")
    })
    // 获取counter累加的值
    println(this.acc.getCounter)
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
