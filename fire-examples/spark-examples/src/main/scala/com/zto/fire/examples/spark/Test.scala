package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.common.util.{DateFormatUtils, PropUtils}
import com.zto.fire.examples.spark.jdbc.JdbcTest.tableName
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.BaseSparkStreaming


/**
 * 基于Fire进行Spark Streaming开发
 */
object Test extends BaseSparkStreaming {

  override def process: Unit = {
    logger.error("driver打印：" + PropUtils.getString("fire.rest.url"))
    val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, DateFormatUtils.formatCurrentDateTime(), 10.0, 1))
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, DateFormatUtils.formatCurrentDateTime(), 10.0, 1), keyNum = 2)
    (1 to 1000).foreach(count => {
      this.fire.createRDD(1 to 1000, 10).foreachPartition(it => {
        HBaseConnector.scanResultScanner("fire_test_1", "1", "10")
        HBaseConnector.scanResultScanner("fire_test_1", "1", "10", keyNum = 2)
      })
      Thread.sleep(1000)
    })
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
