package com.zto.bigdata.spark.rest

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.core.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.rest.RestCase
import com.zto.bigdata.spark.common.util.{GlobalConstants, StringsUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.storage.StorageLevel
import spark.{Request, Response}

/**
  * 使用封装后的api
  */
object RestTest2 extends BaseSparkStreaming {
  val topics = Set("thrall2", "thrall3", "thrall4", "thrall5", "thrall6", "thrall7", "thrall8", "thrall9")
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val tableName = "dw_sz_zto_site_senda_bills"

  def main(args: Array[String]): Unit = {
    this.init(20L, false)
    // 指定端口号，注册新的restful地址
    this.restfulRegister.port(10010)
      .addRest(RestCase("get", "/count", this.rest))
      .addRest(RestCase("get", "/ui", this.ui))
      .startRestServer
    // this.runAsThread(this.kafka)
  }

  def ui(request: Request, response: Response): AnyRef = {
    val line = new StringBuilder()
    val consoleLine = new StringBuilder()
    this.webUI.split(",").foreach(url => {
      line.append(StringsUtils.hrefTag(url) + StringsUtils.brTag(""))
      consoleLine.append(url + "\n")
    })

    println(GlobalConstants.PS1.wrap(consoleLine.toString(), GlobalConstants.PS1.BLUE, GlobalConstants.PS1.UNDER_LINE))
    line.toString()
  }

  def rest(request: Request, response: Response): AnyRef = {
    println(request.body())
    this.spark.sql("show tables").show()
    request.body()
  }

  def kafka: Unit = {
    val dstream = this.ssc.createDirectStream(this.kafkaParams(this.appName + "2", this.brokers, GlobalConstants.KafkaConf.offsetLargest, false), this.topics, StorageLevel.NONE)
    dstream.foreachRDD((rdd, time) => {
      this.spark.kafkaJson2DF(rdd, classOf[Senda]).count//.writeStreaming2Carbon("default", tableName, time)
    })

    this.ssc.startAwaitTermination()
  }

}
