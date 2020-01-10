package com.zto.fire.flink.ext.core.stream

import com.zto.fire.flink.util.FlinkSingletonFactory
import org.apache.flink.api.common.accumulators.{Accumulator, IntCounter, SimpleAccumulator}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._

/**
 * 用于对Flink DataStream的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class DataStreamExt[T](dataStream: DataStream[T]) {
  lazy val tableEnv = FlinkSingletonFactory.getStreamTableEnv

  /**
   * 将流注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    val table = this.dataStream.toTable(this.tableEnv)
    this.tableEnv.registerTable(tableName, table)
    table
  }

  /**
   * 预先注册flink累加器
   *
   * @param acc
   * 累加器实例
   * @param name
   * 累加器名称
   * @return
   * 注册累加器之后的流
   */
  def registerAcc(acc: SimpleAccumulator[_], name: String): DataStream[String] = {
    this.dataStream.map(new RichMapFunction[T, String] {
      override def open(parameters: Configuration): Unit = {
        this.getRuntimeContext.addAccumulator(name, acc)
      }

      override def map(value: T): String = value.toString
    })
  }
}
