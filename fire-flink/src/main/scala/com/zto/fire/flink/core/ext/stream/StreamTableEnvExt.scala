package com.zto.fire.flink.core.ext.stream

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction

/**
 * 用于对Flink StreamTableEnvironment的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class StreamTableEnvExt(tableEnv: StreamTableEnvironment) {

  /**
   * 执行sql query操作
   *
   * @param sql
   * sql语句
   * @return
   * table对象
   */
  def sql(sql: String): Table = {
    this.tableEnv.sqlQuery(sql)
  }

  /**
   * 注册自定义udf函数
   *
   * @param name
   * 函数名
   * @param function
   * 函数的实例
   */
  def udf(name: String, function: ScalarFunction): Unit = {
    this.tableEnv.registerFunction(name, function)
  }
}
