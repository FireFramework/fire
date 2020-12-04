package com.zto.fire.common.util

import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
 * SQL相关工具类
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-26 15:09
 */
object SQLUtils {
  private[this] val beforeWorld = "(?i)(from|join|update|into table|table|into|exists|desc|like|if)"
  private[this] val reg = s"${beforeWorld}\\s+(\\w+\\.\\w+|\\w+)".r

  /**
   * 利用正则表达式解析SQL中用到的表名
   */
  def tableParse(sql: String): ListBuffer[String] = {
    require(StringUtils.isNotBlank(sql), "sql语句不能为空")

    val tables = ListBuffer[String]()
    // 找出所有beforeWorld中定义的关键字匹配到的后面的表名
    reg.findAllMatchIn(sql.replace("""`""", "")).foreach(tableName => {
      // 将匹配到的数据剔除掉beforeWorld中定义的关键字
      val name = tableName.toString().replaceAll(s"${beforeWorld}\\s+", "").trim
      if (StringUtils.isNotBlank(name)) tables += name
    })

    tables
  }
  
}
