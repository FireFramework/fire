package com.zto.fire.demo

import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkStreaming

import scala.collection.JavaConversions

object Test extends BaseSparkStreaming {

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    val rdd = this.sc.parallelize(1 to 1010, 100)
    while (true) {
      println(s"==============${DateFormatUtils.formatCurrentDateTime()}===============")
      rdd.foreachPartition(i => {
        this.acc.addMultiTimer("hbase", 1)
      })
      JavaConversions.asScalaSet(this.acc.getMultiTimer.cellSet()).foreach(t => println(t.getRowKey + " " + t.getColumnKey + " " + t.getValue))
      println
      Thread.sleep(10000)
    }
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init(100, false)
  }
}
