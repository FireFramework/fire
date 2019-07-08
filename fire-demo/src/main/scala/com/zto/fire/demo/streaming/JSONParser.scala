package com.zto.fire.demo.streaming

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.util.SparkUtils
import com.zto.fire.demo.bean.OrderCommon

/**
  * kafka json解析
  * @author ChengLong 2019-6-26 16:52:58
  */
object JSONParser extends BaseSparkStreaming {


  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD((rdd, time) => {
      println("time===> " + time)
      rdd.kafkaJson2Table("test")
      this.spark.sql("select * from test").show(1, false)
    })

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(30, false)

    this.stop
  }
}
