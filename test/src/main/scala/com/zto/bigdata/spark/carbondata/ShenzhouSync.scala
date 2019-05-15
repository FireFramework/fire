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
  val dbName = "tmp"
  val tableName = "test_senda7"

  def main(args: Array[String]): Unit = {
    this.init()

    if(args.length > 0) {
      spark.dropCarbonTable(this.dbName, this.tableName)
      spark.createCarbonStreamingTable(this.dbName, this.tableName, classOf[Senda])
    }

    this.runAsThread(write2Carbondata)
  }

  /**
    * 数据写入到carbondata中
    */
  def write2Carbondata: Unit = {
    val result = spark.loadKafkaParseJson(classOf[Senda])
  }

}
