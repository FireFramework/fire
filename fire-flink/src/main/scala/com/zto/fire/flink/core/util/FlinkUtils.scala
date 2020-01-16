package com.zto.fire.flink.core.util

import com.google.common.collect.HashBasedTable
import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.util.ValueUtils
import com.zto.fire.flink.core.bean.FlinkTableSchema
import org.apache.flink.types.Row

/**
 * flink相关工具类
 *
 * @author ChengLong 2020年1月16日 16:28:23
 * @since 0.4.1
 */
object FlinkUtils {
  // 维护schema、fieldName与fieldIndex关系
  private[this] val schemaTable = HashBasedTable.create[FlinkTableSchema, String, Int]

  /**
   * 将schema、fieldName与fieldIndex信息维护到table中
   */
  private[this] def extendSchemaTable(schema: FlinkTableSchema): Unit = {
    if (ValueUtils.isNotEmpty(schema) && !schemaTable.containsRow(schema)) {
      for (i <- 0 until schema.getFieldCount) {
        schemaTable.put(schema, schema.getFieldName(i).get(), i)
      }
    }
  }

  /**
   * 将Row转为自定义bean，以JavaBean中的Field为基准
   * bean中的field名称要与DataFrame中的field名称保持一致
   *
   * @return
   */
  def flinkRowToBean[T](schema: FlinkTableSchema, row: Row, clazz: Class[T]): T = {
    val obj = clazz.newInstance()
    if (row != null && clazz != null) {
      try {
        this.extendSchemaTable(schema)
        clazz.getDeclaredFields.foreach(field => {
          field.setAccessible(true)
          val anno = field.getAnnotation(classOf[FieldName])
          val begin = if (anno == null) true else !anno.disuse()
          if (begin) {
            val fieldName = if (anno != null && ValueUtils.isNotEmpty(anno.value())) anno.value().trim else field.getName
            if (this.schemaTable.contains(schema, fieldName)) {
              val fieldIndex = this.schemaTable.get(schema, fieldName)
              field.set(obj, row.getField(fieldIndex))
            }
          }
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    obj
  }
}
