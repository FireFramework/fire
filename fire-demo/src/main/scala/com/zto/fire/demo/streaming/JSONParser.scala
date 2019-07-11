package com.zto.fire.demo.streaming

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
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

    dstream.foreachRDD(rdd => {
      if (rdd.isNotEmpty) {
        // 一、将json解析并注册为临时表，默认不cache临时表
        rdd.kafkaJson2Table("test", cacheTable = true)
        // toLowerDF表示将大写的字段转为小写
        this.spark.sql("select * from test").toLowerDF.show(1, false)
        this.spark.sql("select after.* from test").toLowerDF.show(1, false)
        this.spark.sql("select after.* from test where after.platformid=1").toLowerDF.show(1, false)

        // 二、直接将json按指定的schema解析（只解析after），fieldNameUpper=true表示按大写方式解析，并自动转为小写
        rdd.kafkaJson2DF(classOf[OrderCommon], fieldNameUpper = true).show(2, false)
        // 递归解析所有指定的字段，包括before、table、offset等字段
        rdd.kafkaJson2DF(classOf[OrderCommon], parseAll = true, fieldNameUpper = true, isMySQL = false).show(2, false)

        this.spark.uncache("test")
        rdd.kafkaCommitOffsets(dstream)
      }
    })

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
    this.stop
  }
}
