package com.zto.fire.flink.core.ext.batch

import com.zto.fire.flink.core.util.FlinkSingletonFactory
import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.Table

/**
 * 用于对Flink DataSet的API库扩展
 *
 * @author ChengLong 2020年1月15日 16:35:03
 * @since 0.4.1
 */
class DataSetExt[T](dataSet: DataSet[T]){
  lazy val tableEnv = FlinkSingletonFactory.getBatchTableEnv

  /**
   * 将DataSet注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    val table = this.tableEnv.fromDataSet(this.dataSet)
    this.tableEnv.createTemporaryView(tableName, table)
    table
  }

  /**
   * 设置并行度
   */
  def repartition(parallelism: Int): DataSet[T] = {
    this.dataSet.setParallelism(parallelism)
  }

  /**
   * 将DataSet转为Table
   */
  def toTable: Table = {
    this.tableEnv.fromDataSet(this.dataSet)
  }


}
