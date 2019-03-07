package com.zto.bigdata.spark.common.util

import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.anno.FieldName
import org.apache.commons.lang3.StringUtils

/**
  * carbondata相关工具类
  *
  * @author Chenglong 2019-3-7 15:28:23
  */
object CarbondataUtils {

  /**
    * 根据指定的javabean，构建carbondata表sql
    *
    * @param tableName
    * 表名
    * @param tableSchema
    * 表的schema信息，与javabean对应
    * @param isStreaming
    * 是否创建成streaming表
    * @return
    */
  def buildCreateTableSQL(tableName: String, tableSchema: Class[_], isStreaming: Boolean = false): String = {
    val schema = ReflectionUtils.getAllFields(tableSchema).toScalaMap
    val sql = new StringBuilder(s"CREATE TABLE IF NOT EXISTS ${tableName}(\n")
    schema.foreach(t => {
      val fieldType = t._2.getType
      val field = t._2.getAnnotation(classOf[FieldName])
      val comment = if (field != null && StringUtils.isNotBlank(field.comment())) s" COMMENT '${field.comment()}'" else ""
      sql.append(s"\t${t._1} ${map2CarbonType(fieldType)}${comment},\n")
    })
    val finallySQL = sql.substring(0, sql.length - 2) + ")" +
      s"""
        |STORED BY 'carbondata'
        |${if(isStreaming) "TBLPROPERTIES('streaming' = 'true')" else ""}
      """.stripMargin
    println(finallySQL)
    finallySQL
  }

  /**
    * 给定java类型，获取对应的carbondata类型
    *
    * @param fieldType
    * @return
    */
  def map2CarbonType(fieldType: Class[_]): String = {
    if (fieldType eq classOf[String]) "string"
    else if (fieldType eq classOf[java.lang.Integer]) "int"
    else if (fieldType eq classOf[java.lang.Double]) "double"
    else if (fieldType eq classOf[java.lang.Long]) "bigint"
    else if (fieldType eq classOf[java.math.BigDecimal]) "decimal"
    else if (fieldType eq classOf[java.lang.Float]) "double"
    else if (fieldType eq classOf[java.lang.Boolean]) "boolean"
    else if (fieldType eq classOf[java.lang.Short]) "smallint"
    else if (fieldType eq classOf[java.util.Date]) "date"
    else "string"
  }
}
