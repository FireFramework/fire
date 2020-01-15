package com.zto.fire.flink.core.ext

import com.zto.fire.flink.core.ext.batch.{BatchExecutionEnvExt, BatchTableEnvExt, DataSetExt}
import com.zto.fire.flink.core.ext.stream.{DataStreamExt, StreamExecutionEnvExt, StreamTableEnvExt, StreamTableExt}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}

import scala.reflect.ClassTag

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
  implicit class StreamTableExtBridge(table: Table) extends StreamTableExt(table) {

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
}
