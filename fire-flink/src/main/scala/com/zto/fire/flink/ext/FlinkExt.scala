package com.zto.fire.flink.ext

import com.zto.fire.flink.ext.core.{DataStreamExt, StreamExecutionEnvironmentExt, StreamTableEnvironmentExt, TableExt}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * Flink扩展工具类，利用隐式转换对已有的类追加自定义函数
 * Created by ChengLong on 2020年1月6日 16:31:56.
 */
object FlinkExt {

  /**
   * StreamExecutionEnvironment扩展
   *
   * @param env
   * StreamExecutionEnvironment对象
   */
  implicit class StreamExecutionEnvironmentExtBridge(env: StreamExecutionEnvironment) extends StreamExecutionEnvironmentExt(env) {

  }

  /**
   * StreamTableEnvironment扩展
   *
   * @param tableEnv
   * StreamTableEnvironment对象
   */
  implicit class StreamTableEnvironmentExtBridge(tableEnv: StreamTableEnvironment) extends StreamTableEnvironmentExt(tableEnv) {

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
  implicit class TableExtBridge(table: Table) extends TableExt(table) {

  }

}