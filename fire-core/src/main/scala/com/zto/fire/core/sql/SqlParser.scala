package com.zto.fire.core.sql

import com.zto.fire.common.conf.FireFrameworkConf.{buriedPointDatasourceEnable, buriedPointDatasourceInitialDelay, buriedPointDatasourcePeriod}
import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.common.util.{DatasourceManager, TableMeta, ThreadUtils}
import org.slf4j.LoggerFactory
import com.zto.fire.predef._

import java.util.concurrent.{CopyOnWriteArraySet, ScheduledExecutorService, TimeUnit}
import scala.collection.mutable

/**
 * 用于各引擎的SQL解析
 *
 * @author ChengLong 2021-6-18 16:28:50
 * @since 2.0.0
 */
trait SqlParser {
  // 用于存放解析后的库表类
  lazy val tableMap = new JConcurrentHashMap[String, TableMeta]()
  protected[fire] lazy val hiveTableMap = new JConcurrentHashMap[String, Boolean]()
  protected lazy val buffer = new CopyOnWriteArraySet[String]()
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)
  protected lazy val threadPool = ThreadUtils.createThreadPool("FireSqlParser", ThreadPoolType.SCHEDULED)
  this.sqlParse

  /**
   * 周期性的解析SQL语句
   */
  private def sqlParse: Unit = {
    if (buriedPointDatasourceEnable) {
      this.threadPool.asInstanceOf[ScheduledExecutorService].scheduleWithFixedDelay(() => {
        this.buffer.foreach(sql => this.sqlParser(sql))
        DatasourceManager.addTableMeta(this.tableMap)
        this.clear
      }, buriedPointDatasourceInitialDelay, buriedPointDatasourcePeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 清理解析后的SQL数据
   */
  private[this] def clear: Unit = {
    this.buffer.clear()
    this.tableMap.clear()
  }

  /**
   * 将待解析的SQL添加到buffer中
   */
  def sqlParse(sqls: String *): Unit = {
    if (buriedPointDatasourceEnable && noEmpty(sqls)) {
      this.buffer ++= sqls.toSet
    }
  }

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
