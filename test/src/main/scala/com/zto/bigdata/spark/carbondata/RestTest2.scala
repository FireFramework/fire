package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.ext.{BaseSparkCore, BaseSparkStreaming}
import com.zto.bigdata.spark.common.util.GlobalConstants
import org.apache.spark.storage.StorageLevel
import spark.Spark._
import spark.{Request, Response, Route}

object RestTest2 extends BaseSparkStreaming {
  val topics = Set("thrall2", "thrall3", "thrall4", "thrall5", "thrall6", "thrall7", "thrall8", "thrall9")
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val storePath = "hdfs://appcluster/user/CarbonStore"
  val tableName = "dw_sz_zto_site_senda_bills"

  def main(args: Array[String]): Unit = {
    this.init(20L, null, false)
    this.runAsThread(this.rest)
    this.runAsThread(this.kafka)
  }

  def kafka: Unit = {
    val dstream = this.ssc.createDirectStream(this.kafkaParams(this.brokers, this.appName + "2", GlobalConstants.KafkaConf.offsetLargest, false), this.topics, StorageLevel.NONE)
    dstream.foreachRDD((rdd, time) => {
      this.parseJson2DataFrame(rdd, classOf[Senda]).writeStreaming2Carbon("default", tableName, time)
    })

    this.ssc.startAwaitTermination()
  }

  def rest: Unit = {
    get("/count", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        println(s"ip:${request.ip()} 调用/count接口")
        spark.sql("select count(1) from dw_sz_zto_site_senda_bills").show()
        return request.body()
      }
    })

    get("/select", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        println(s"ip:${request.ip()} 调用/select接口")
        spark.sql("select * from dw_sz_zto_site_senda_bills").show(10)
        return ""
      }
    })

    get("/sql", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        println(s"ip:${request.ip()} 调用/sql接口")
        spark.sql(request.body()).show(100)
        return "执行sql成功"
      }
    })
  }

}
