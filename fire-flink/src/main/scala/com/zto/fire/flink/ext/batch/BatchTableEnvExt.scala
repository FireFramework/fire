package com.zto.fire.flink.ext.batch

import com.zto.fire.jdbc.JdbcConnectorBridge
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

/**
 * 用于flink BatchTableEnvironment API库扩展
 *
 * @author ChengLong 2020年1月9日 13:52:16
 * @since 0.4.1
 */
private[fire] class BatchTableEnvExt(env: BatchTableEnvironment) extends JdbcConnectorBridge {

  /**
   * 执行sql query操作
   *
   * @param sql
   * sql语句
   * @return
   * table对象
   */
  def sql(sql: String): Table = {
    this.env.sqlQuery(sql)
  }

}
