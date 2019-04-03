package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.ext.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.{GlobalConstants, SparkUtils}
import org.apache.spark.storage.StorageLevel

/**
  * 以streaming方式写carbondata
  *
  * @author ChengLong 2019-4-2 19:17:56
  */
object ShenzhouSyncStreaming extends BaseSparkStreaming {
  val topics = SparkUtils.topicSplit("thrall2, thrall3, thrall4, thrall5, thrall6, thrall7, thrall8, thrall9")
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val tableName = "dw_sz_zto_site_senda_bills2"

  def main(args: Array[String]): Unit = {
    this.init(20L, null, false)

    if (args != null && args.length > 0) {
      this.spark.dropCarbonTable(GlobalConstants.SparkConf.defaultDB, this.tableName)
      this.spark.createCarbonStreamingTable(GlobalConstants.SparkConf.defaultDB, this.tableName, classOf[Senda])
    }

    this.runAsThreadLoop(this.printCount, 60, 1,true)
    this.runAsThread(this.kafka)
  }

  def kafka: Unit = {
    val dstream = this.ssc.createDirectStream(this.kafkaParams(this.appName + "2", this.brokers, GlobalConstants.KafkaConf.offsetLargest, false), this.topics, StorageLevel.NONE)
    dstream.foreachRDD((rdd, time) => {
      this.parseJson2DataFrame(rdd, classOf[Senda]).writeStreaming2Carbon(GlobalConstants.SparkConf.defaultDB, tableName, time)
    })

    this.ssc.startAwaitTermination()
  }


  /**
    * 统计表中的记录数
    */
  def printCount: Unit = {
    spark.sql(s"select count(1) from ${GlobalConstants.SparkConf.defaultDB}.$tableName").show()
  }
}
