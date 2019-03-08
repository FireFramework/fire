package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.bean.SiteSendMqDTO
import com.zto.bigdata.spark.common.ext.BaseSparkCore
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.CarbondataUtils
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.collection.mutable


/**
  * 神州数据同步
  */
object ShenzhouSync extends BaseSparkCore {
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val topicSet = "thrall2, thrall3, thrall4, thrall5, thrall6, thrall7, thrall8, thrall9"

  val warehouse = "hdfs://appcluster/user/spark/"
  val tableName = "dw_sz_zto_site_senda_bills"

  def main(args: Array[String]): Unit = {
    this.spark = SparkSession
      .builder()
      .appName("ShenzhouSync")
      .getOrCreateCarbonSession("hdfs://appcluster/user/CarbonStore")
    spark.sparkContext.setLogLevel("ERROR")

    if(args.length > 0) {
      spark.sql(s"DROP TABLE IF EXISTS ${tableName}")
      spark.sql(CarbondataUtils.buildCreateTableSQL(this.tableName, classOf[SiteSendMqDTO], true))
    }

    this.runAsThread(write2Carbondata)
    this.runAsThreadLoop(this.printCount, 10, true)
  }

  /**
    * 数据写入到carbondata中
    */
  def write2Carbondata: Unit = {
    val result = spark
      .loadKafkaParseJson(brokers, mutable.HashMap[String, String]("subscribe" -> topicSet, "failOnDataLoss" -> "false", "startingOffsets" -> "latest"), classOf[SiteSendMqDTO])

    result.repartition(200).writeStream2Carbon("default", tableName, Trigger.ProcessingTime("60 seconds"))
  }

  /**
    * 统计表中的记录数
    */
  def printCount: Unit = {
    spark.sql(s"select count(1) from $tableName").show()
  }

}
