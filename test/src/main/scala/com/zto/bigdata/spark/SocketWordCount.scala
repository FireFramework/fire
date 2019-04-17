package com.zto.bigdata.spark

import com.zto.bigdata.spark.common.ext.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.{GlobalConstants, SparkUtils}
import org.apache.spark.storage.StorageLevel

/**
  * 以streaming方式写carbondata
  *
  * @author ChengLong 2019-4-2 19:17:56
  */
object SocketWordCount extends BaseSparkStreaming {
  val topics = SparkUtils.topicSplit("thrall2, thrall3, thrall4, thrall5, thrall6, thrall7, thrall8, thrall9")
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val dbName = "dw"
  val tableName = "dw_sz_zto_site_senda_bills2"

  def main(args: Array[String]): Unit = {
    this.restfulRegister.port(10010).startRestServer
    this.init(20L, false)
  }


  /**
    * Spark处理过程
    * 注：此方法会被自动调用，若需使用
    * checkpoint中的数据，则子类必须复写该方法
    */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream(this.kafkaParams(this.appName + "2", this.brokers, GlobalConstants.KafkaConf.offsetLargest, false), this.topics, StorageLevel.NONE)
    dstream.foreachRDD(rdd => {
      println(rdd.count())
    })
    this.ssc.start()
    this.ssc.awaitTermination()
  }

}
