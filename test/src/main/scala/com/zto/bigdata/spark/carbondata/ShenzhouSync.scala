package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.ext.BaseStructuredStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import org.apache.spark.sql.streaming.Trigger

import scala.collection.mutable


/**
  * 神州数据同步
  */
object ShenzhouSync extends BaseStructuredStreaming {
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val topicSet = "thrall2, thrall3, thrall4, thrall5, thrall6, thrall7, thrall8, thrall9"

  val dbName = "dw"
  val tableName = "dw_sz_zto_site_senda_bills2"

  def main(args: Array[String]): Unit = {
    this.init()
    spark.sparkContext.setLogLevel("ERROR")

    if(args.length > 0) {
      spark.sql(s"DROP TABLE IF EXISTS ${dbName}.${tableName}")
      spark.dropCarbonTable(this.dbName, this.tableName)
      spark.createCarbonStreamingTable(this.dbName, this.tableName, classOf[Senda])
    }

    this.runAsThread(write2Carbondata)
    this.runAsThreadLoop(this.printCount, 60, 1, true)
  }

  /**
    * 数据写入到carbondata中
    */
  def write2Carbondata: Unit = {
    val result = spark
      .loadKafkaParseJson(brokers, mutable.HashMap[String, String]("subscribe" -> topicSet, "failOnDataLoss" -> "false", "startingOffsets" -> "latest"), classOf[Senda])

    result.repartition(200).writeStream2Carbon(this.dbName, this.tableName, Trigger.ProcessingTime("60 seconds"))
  }

  /**
    * 统计表中的记录数
    */
  def printCount: Unit = {
    spark.sql(s"select count(1) from ${this.dbName}.$tableName").show()
  }

}
