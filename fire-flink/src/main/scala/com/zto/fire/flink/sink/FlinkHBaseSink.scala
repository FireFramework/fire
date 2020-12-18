package com.zto.fire.flink.sink

import com.zto.fire.common.conf.FireHBaseConf
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.bean.HBaseBaseBean

import scala.collection.JavaConversions

/**
 * flink HBase sink组件，底层基于HBaseOper
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-5-25 16:06:15
 */
abstract class FlinkHBaseSink[IN](tableName: String,
                                  insertEmpty: Boolean = true,
                                  batch: Int = 100,
                                  multiVersion: Boolean = false,
                                  flushInterval: Long = 10000,
                                  keyNum: Int = 1) extends BaseFlinkSink[IN, HBaseBaseBean[_]](batch, flushInterval) {

  // hbase操作失败时允许最大重试次数
  this.maxRetry = FireHBaseConf.hbaseMaxRetry()

  /**
   * 将数据sink到hbase
   * 该方法会被flush方法自动调用
   */
  override def sink: Unit = {
    // HBaseOper.insert(this.tableName, this.buffer, this.insertEmpty, this.multiVersion)
    // HBaseConnector.insert(this.tableName, JavaConversions.asScalaBuffer(this.buffer), this.keyNum)
  }
}
