package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.ext.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.GlobalConstants
import org.apache.spark.storage.StorageLevel

object ShenzhouSyncStreaming extends BaseSparkStreaming {
  val topics = Set("thrall2", "thrall3", "thrall4", "thrall5", "thrall6", "thrall7", "thrall8", "thrall9")
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val storePath = "hdfs://appcluster/user/CarbonStore"
  val tableName = "dw_sz_zto_site_senda_bills"

  def main(args: Array[String]): Unit = {
    this.init(20L, null, false)

    if(args != null && args.length > 0) {
      this.spark.dropCarbonTable("default", this.tableName)
      this.spark.createCarbonStreamingTable("default", this.tableName, classOf[Senda])
    }

    this.runAsThreadLoop(this.printCount, 60, true)
    this.runAsThread(this.kafka)
  }

  def kafka: Unit = {
    val dstream = this.ssc.createDirectStream(this.kafkaParams(this.brokers, this.appName + "2", GlobalConstants.KafkaConf.offsetLargest, false), this.topics, StorageLevel.NONE)
    dstream.foreachRDD((rdd, time) => {
      this.parseJson2DataFrame(rdd, classOf[Senda]).writeStreaming2Carbon("default", tableName, time)
    })

    this.ssc.startAwaitTermination()
  }


  /**
    * 统计表中的记录数
    */
  def printCount: Unit = {
    spark.sql(s"select count(1) from $tableName").show()
  }
}
