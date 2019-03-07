package com.zto.bigdata.spark.carbondata

import java.util

import com.zto.bigdata.spark.CarbonQL
import com.zto.bigdata.spark.bean.SiteSendMqDTO
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{CarbonEnv, Encoders, SparkSession}
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.CarbondataUtils

import scala.collection.mutable


/**
  * 神州数据同步
  */
object ShenzhouSync {
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val topicSet = "thrall2"

  val warehouse = "hdfs://appcluster/user/spark/"
  val metastore = "hdfs://appcluster/user/spark/metastore/"
  val chekpoint = "hdfs://appcluster/user/spark/chkpoint"
  val tableName = "dw_sz_zto_site_senda_bills"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ShenzhouSync")
      .getOrCreateCarbonSession("hdfs://appcluster/user/CarbonStore")
    spark.sparkContext.setLogLevel("ERROR")

    if(args.length > 0) {
      spark.sql(s"DROP TABLE IF EXISTS ${tableName}")
      spark.sql(CarbondataUtils.buildCreateTableSQL(this.tableName, classOf[SiteSendMqDTO], true))
    }

    spark.sql(s"select * from ${tableName} limit 2").show()
    spark.sql(s"select count(1) from ${tableName}").show()
    spark.sql("show tables").show(2)

    val result = spark
      .loadKafkaParseJson(this.brokers, mutable.HashMap[String, String]("subscribe" -> topicSet), classOf[SiteSendMqDTO])

    val carbonTable = CarbonEnv.getCarbonTable(Some("default"), s"${tableName}")(spark)
    val tablePath = carbonTable.getTablePath

    val table = result.writeStream
      .format("carbondata")
      .trigger(ProcessingTime("5 seconds"))
      .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(tablePath))
      .option("dbName", "default")
      .option("tableName", this.tableName)
      .option(CarbonStreamParser.CARBON_STREAM_PARSER, CarbonStreamParser.CARBON_STREAM_PARSER_ROW_PARSER)
      .start()

    table.awaitTermination()
  }
}
