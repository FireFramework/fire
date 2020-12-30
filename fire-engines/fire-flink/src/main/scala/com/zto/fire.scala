package com.zto

import com.zto.fire.core.ext.BaseFireExt
import com.zto.fire.flink.ext.batch.{BatchExecutionEnvExt, BatchTableEnvExt, DataSetExt}
import com.zto.fire.flink.ext.stream._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.types.Row

/**
 * 预定义fire框架中的扩展工具
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-22 13:51
 */
package object fire extends BaseFireExt {

  /**
   * StreamExecutionEnvironment扩展
   *
   * @param env
   * StreamExecutionEnvironment对象
   */
  implicit class StreamExecutionEnvExtBridge(env: StreamExecutionEnvironment) extends StreamExecutionEnvExt(env) {

  }

  /**
   * StreamTableEnvironment扩展
   *
   * @param tableEnv
   * StreamTableEnvironment对象
   */
  implicit class StreamTableEnvExtBridge(tableEnv: StreamTableEnvironment) extends StreamTableEnvExt(tableEnv) {

  }


  /**
   * DataStream扩展
   *
   * @param dataStream
   * DataStream对象
   */
  implicit class DataStreamExtBridge[T](dataStream: DataStream[T]) extends DataStreamExt(dataStream) {

  }

  /**
   * Table扩展
   *
   * @param table
   * Table对象
   */
  implicit class StreamTableExtBridge(table: Table) extends TableExt(table) {

  }

  /**
   * BatchTableEnvironment扩展
   *
   * @param tableEnv
   * BatchTableEnvironment对象
   */
  implicit class BatchTableEnvExtBridge(tableEnv: BatchTableEnvironment) extends BatchTableEnvExt(tableEnv) {

  }


  /**
   * ExecutionEnvironment扩展
   *
   * @param env
   * ExecutionEnvironment对象
   */
  implicit class BatchExecutionEnvExtBridge(env: ExecutionEnvironment) extends BatchExecutionEnvExt(env) {

  }

  /**
   * DataSet扩展
   *
   * @param dataSet
   * DataSet对象
   */
  implicit class DataSetExtBridge[T](dataSet: DataSet[T]) extends DataSetExt(dataSet) {

  }

  /**
   * Row扩展
   */
  implicit class RowExtBridge(row: Row) extends RowExt(row) {

  }
}
