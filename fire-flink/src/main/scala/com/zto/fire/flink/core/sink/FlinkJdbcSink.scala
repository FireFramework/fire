package com.zto.fire.flink.core.sink

import com.zto.fire.common.db.JdbcOper

import scala.collection.JavaConversions

/**
 * flink jdbc sink组件，底层基于JdbcOper
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-05-22 10:37
 */
abstract class FlinkJdbcSink[IN](sql: String,
                                 batch: Int = 10,
                                 flushInterval: Long = 1000,
                                 keyNum: Int = 1) extends BaseFlinkSink[IN, Seq[Any]](batch, flushInterval) {

  /**
   * 将数据sink到jdbc
   * 该方法会被flush方法自动调用
   */
  override def sink: Unit = {
    JdbcOper.executeBatch(sql, JavaConversions.asScalaBuffer(this.buffer), keyNum = keyNum)
  }
}
