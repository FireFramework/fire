package com.zto.fire.demo.sql

import com.zto.fire.common.util.GlobalConstants
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.util.SparkUtils
import com.zto.fire.demo.bean.Senda

object SqlEngine extends BaseSparkStreaming {
  val topics = SparkUtils.topicSplit("thrall2, thrall3, thrall4, thrall5, thrall6, thrall7, thrall8, thrall9")
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"

  def main(args: Array[String]): Unit = {
    this.restfulRegister.startRestServer
    this.init(10, false)
    this.runAsThread(this.kafka)
  }

  def kafka: Unit = {
    val dstream = this.ssc.createDirectStream(this.kafkaParams(this.appName, this.brokers, GlobalConstants.KafkaConf.offsetLargest, false), this.topics)
    dstream.foreachRDD((rdd, time) => {
      // this.parseJson2DataFrame(rdd, classOf[Senda]).writeStreaming2Carbon(this.dbName, tableName, time)
      this.spark.kafkaJson2DF(rdd, classOf[Senda]).createOrReplaceTempView("tmp")
      this.spark.sql("select count(1) from tmp").show
      this.spark.sql("select bill_code from tmp limit 2").show(false)
    })

    this.ssc.startAwaitTermination()
  }
}
