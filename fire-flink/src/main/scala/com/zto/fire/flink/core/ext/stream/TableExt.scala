package com.zto.fire.flink.core.ext.stream

import com.zto.fire.common.util.ValueUtils
import com.zto.fire.flink.core.bean.FlinkTableSchema
import com.zto.fire.flink.core.util.FlinkSingletonFactory
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * 用于flink StreamTable API库扩展
 *
 * @author ChengLong 2020年1月9日 13:52:16
 * @since 0.4.1
 */
class TableExt(table: Table) {
  lazy val streamTableEnv = FlinkSingletonFactory.getStreamTableEnv
  lazy val batchTableEnv = FlinkSingletonFactory.getBatchTableEnv

  /**
   * 逐条打印每行记录
   */
  def show: Unit = {
    this.table.addSink(row => println(row))
  }

  /**
   * 获取表的schema包装类，用于flinkRowToBean
   *
   * @return
   * fire包装后的表schema信息
   */
  def getTableSchema: FlinkTableSchema = {
    new FlinkTableSchema(table.getSchema)
  }

  /**
   * 将Table转为追加流
   */
  def toAppendStream[T]: DataStream[Row] = {
    this.streamTableEnv.toAppendStream[Row](this.table)
  }

  /**
   * 将Table转为Retract流
   */
  def toRetractStream[T]: DataStream[(Boolean, Row)] = {
    this.streamTableEnv.toRetractStream[Row](this.table)
  }

  /**
   * 将Table转为DataSet
   */
  def toDataSet[T]: DataSet[Row] = {
    ValueUtils.requireNonNull(this.batchTableEnv, "BatchTableEnvironment")
    this.batchTableEnv.toDataSet[Row](this.table)
  }

  /**
   * 将流注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    if (this.streamTableEnv != null) {
      this.streamTableEnv.createTemporaryView(tableName, table)
    } else if (this.batchTableEnv != null) {
      this.batchTableEnv.createTemporaryView(tableName, table)
    } else {
      throw new NullPointerException("table environment对象实例为空，请检查")
    }
    table
  }
}
