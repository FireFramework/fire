package com.zto.fire.demo.zrc

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

/**
  * Zrc联调程序
  * @author ChengLong 2019-7-8 09:06:56
  */
object ZrcDemo extends BaseSparkStreaming {

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.print(1)
    dstream.foreachRDD(rdd => {
      println("count=" + rdd.count())
    })
    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(30, false)
    this.stop
  }
}
