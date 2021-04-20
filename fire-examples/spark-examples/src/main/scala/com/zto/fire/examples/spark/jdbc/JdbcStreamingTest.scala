package com.zto.fire.examples.spark.jdbc

import com.zto.fire._
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.spark.BaseSparkStreaming

object JdbcStreamingTest extends BaseSparkStreaming {
  val tableName = "t_hosts"

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()

    dstream.repartition(5).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val sql = s"select id from $tableName limit 1"
        JdbcConnector.executeQueryCall(sql, callback = _ => 1, keyNum = 3)
      })
    })

    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
