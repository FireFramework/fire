package com.zto.fire.core.sql

import com.zto.fire.common.enu.{Datasource, Operation}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * 用于各引擎的SQL解析
 *
 * @author ChengLong 2021-6-18 16:28:50
 * @since 2.0.0
 */
trait SqlParser {
  // 用于存放解析后的库表类
  protected[fire] lazy val tableMap = new mutable.LinkedHashMap[String, Table]()
  protected[fire] lazy val hiveTableMap = new mutable.HashMap[String, Boolean]()
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * sql解析后的库表信息包装类
   */
  case class Table(db: String = "", name: String = "", var partition: mutable.Map[String, String] = mutable.Map.empty, var catalog: Datasource = Datasource.VIEW, operation: Operation = Operation.SELECT, var properties: mutable.Map[String, String] = mutable.Map.empty)

  /**
   * 用于解析给定的SQL语句
   */
  def sqlParser(sql: String): Unit

  /**
   * 用于判断给定的表是否为临时表
   */
  def isTempView(dbName: String = null, tableName: String): Boolean

  /**
   * 用于判断给定的表是否为hive表
   */
  def isHiveTable(dbName: String = null, tableName: String): Boolean

  /**
   * 将库表名转为字符串
   */
  def tableIdentifier(dbName: String, tableName: String): String = s"$dbName.$tableName"
}
