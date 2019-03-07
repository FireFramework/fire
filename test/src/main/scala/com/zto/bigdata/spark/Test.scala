package com.zto.bigdata.spark

import com.zto.bigdata.spark.bean.SiteSendMqDTO
import com.zto.bigdata.spark.common.anno.FieldName
import com.zto.bigdata.spark.common.util.{ReflectionUtils, SparkUtils}
import com.zto.bigdata.spark.common.ext.SparkExt._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.DataTypes

object Test {

  def main(args: Array[String]): Unit = {
    val schema = ReflectionUtils.getAllFields(classOf[SiteSendMqDTO]).toScalaMap
    val sql = new StringBuilder("CREATE TABLE dw_sz_zto_site_senda_bills(\n")
    schema.foreach(t => {
      val fieldType = t._2.getType
      val field = t._2.getAnnotation(classOf[FieldName])
      val comment = if(field != null && StringUtils.isNotBlank(field.comment())) s" COMMENT '${field.comment()}'" else ""
      sql.append(s"\t${t._1} ${map2CarbonType(fieldType)}${comment},\n")
    })
    val isStreaming = true
    val finallySQL = sql.substring(0, sql.length - 2) + ")" +
      s"""
         |STORED BY 'carbondata'
         |${if(isStreaming) "TBLPROPERTIES('streaming' = 'true')" else ""}
      """.stripMargin
    println(finallySQL)
  }

  /**
    * 给定java类型，获取对应的carbondata类型
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
