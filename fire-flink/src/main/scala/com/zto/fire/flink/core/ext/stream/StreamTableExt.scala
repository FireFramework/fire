package com.zto.fire.flink.core.ext.stream

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * 用于flink StreamTable API库扩展
 *
 * @author ChengLong 2020年1月9日 13:52:16
 * @since 0.4.1
 */
class StreamTableExt(table: Table) {

  /**
   * 逐条打印每行记录
   */
  def show: Unit = {
    this.table.addSink(row => println(row))
  }
}
