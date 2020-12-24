package com.zto.fire.flink.ext.stream

import com.zto.fire.flink.bean.FlinkTableSchema
import com.zto.fire.flink.util.FlinkUtils
import org.apache.flink.types.Row

/**
 * 用于flink Row API库扩展
 *
 * @author ChengLong 2020年3月30日 17:00:05
 * @since 0.4.1
 */
private[fire] class RowExt(row: Row) {

  /**
   * 将flink的row转为指定类型的JavaBean
   * @param schema
   *               表的schema
   * @param clazz
   *              目标JavaBean类型
   */
  def rowToBean[T](schema: FlinkTableSchema, clazz: Class[T]): T = {
    FlinkUtils.rowToBean(schema, row, clazz)
  }
}
