package com.zto.fire.flink.sink

import com.zto.fire.predef._
import com.zto.fire.common.conf.FireJdbcConf
import com.zto.fire.jdbc.JdbcConnector

/**
 * flink jdbc sink组件，底层基于JdbcConnector
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-05-22 10:37
 */
abstract class FlinkJdbcSink[IN](sql: String,
                                 batch: Int = 10,
                                 flushInterval: Long = 1000,
                                 keyNum: Int = 1) extends BaseFlinkSink[IN, Seq[Any]](batch, flushInterval) {

  // jdbc操作失败时允许最大重试次数
  this.maxRetry = FireJdbcConf.maxRetry(keyNum)

  /**
   * 将数据sink到jdbc
   * 该方法会被flush方法自动调用
   */
  override def sink: Unit = {
    JdbcConnector.executeBatch(sql, this.buffer, keyNum = keyNum)
  }
}
