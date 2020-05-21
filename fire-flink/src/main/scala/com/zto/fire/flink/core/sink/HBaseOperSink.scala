package com.zto.fire.flink.core.sink

import com.zto.fire.common.bean.{HBaseBaseBean, MultiVersionsBean}
import com.zto.fire.common.db.HBaseOper
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * 自定义HBase sink
 *
 * @param tableName
 * hbase 表名
 * @param insertEmpty
 * 为空的字段是否插入
 * @param multiVersion
 * 是否支持多版本插入
 * @author ChengLong 2020年1月15日 16:11:08
 * @since 0.4.1
 */
class HBaseOperSink[T <: HBaseBaseBean[T]](tableName: String, insertEmpty: Boolean = true, multiVersion: Boolean = false) extends RichSinkFunction[T] {

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    if (multiVersion) {
      HBaseOper.insert(tableName, new MultiVersionsBean(value))
    } else {
      if (insertEmpty) {
        HBaseOper.insert(tableName, value)
      } else {
        HBaseOper.insertIgnoreNull(tableName, value)
      }
    }
  }
}