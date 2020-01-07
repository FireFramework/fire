package com.zto.fire.flink.ext

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * 用于对Flink StreamTableEnvironment的API库扩展
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class StreamTableEnvironmentExt(tableEnv: StreamTableEnvironment) {

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

}
