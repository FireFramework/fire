package com.zto.fire.demo.spark.jdbc

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

object JdbcStreamingTest extends BaseSparkStreaming {
  val tableName = "t_hosts"

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()

    dstream.repartition(5).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        this.mark
        val sql = s"select id from $tableName limit 1"
        this.jdbc.executeQueryCall(sql)
        this.log("查询完成")
      })
    })

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
