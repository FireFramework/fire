package com.zto.fire.flink.ext

import com.zto.fire.flink.util.FlinkSingletonFactory
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * 用于对Flink DataStream的API库扩展
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class DataStreamExt[T](dataStream: DataStream[T]) {
  lazy val tableEnv = FlinkSingletonFactory.getTableEnv

  def createOrReplaceTempView(tableName: String = ""): Table = {
    val table = this.dataStream.toTable(this.tableEnv)
    this.tableEnv.registerTable(tableName, table)
    table
  }
}
