package com.zto.bigdata.spark.rest

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.ext.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.rest.Rest
import com.zto.bigdata.spark.common.util.GlobalConstants
import org.apache.spark.storage.StorageLevel
import spark.{Request, Response}

object RestTest2 extends BaseSparkStreaming {
  val topics = Set("thrall2", "thrall3", "thrall4", "thrall5", "thrall6", "thrall7", "thrall8", "thrall9")
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val storePath = "hdfs://appcluster/user/CarbonStore"
  val tableName = "dw_sz_zto_site_senda_bills"

  def main(args: Array[String]): Unit = {
    this.init(20L,null, false)
    this.restfulRegister.port(10010)
      .addRest(Rest("get", "/count", this.rest))
      .addRest(Rest("post", "/count2", this.rest))
      .startRestServer
    this.runAsThread(this.kafka)
  }

  def rest(request: Request, response: Response): AnyRef = {
    println(request.body())
    this.spark.sql("show tables").show()
    request.body()
  }

  def kafka: Unit = {
    val dstream = this.ssc.createDirectStream(this.kafkaParams(this.appName + "2", this.brokers, GlobalConstants.KafkaConf.offsetLargest, false), this.topics, StorageLevel.NONE)
    dstream.foreachRDD((rdd, time) => {
      this.parseJson2DataFrame(rdd, classOf[Senda]).writeStreaming2Carbon("default", tableName, time)
    })

    this.ssc.startAwaitTermination()
  }

}
