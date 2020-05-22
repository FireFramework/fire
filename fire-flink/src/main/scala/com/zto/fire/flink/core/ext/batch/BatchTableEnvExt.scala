package com.zto.fire.flink.core.ext.batch

import com.zto.fire.core.bridge.JdbcOperBridge
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment

/**
 * 用于flink BatchTableEnvironment API库扩展
 *
 * @author ChengLong 2020年1月9日 13:52:16
 * @since 0.4.1
 */
class BatchTableEnvExt(env: BatchTableEnvironment) extends JdbcOperBridge {

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
