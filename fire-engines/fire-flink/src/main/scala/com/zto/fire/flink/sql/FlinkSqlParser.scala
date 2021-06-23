package com.zto.fire.flink.sql

import com.zto.fire._
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.core.sql.SqlParser
import com.zto.fire.flink.util.FlinkSingletonFactory
import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.{SqlParser => CalciteParser}
import org.apache.flink.sql.parser.SqlProperty
import org.apache.flink.sql.parser.ddl._
import org.apache.flink.sql.parser.dml._
import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import org.apache.flink.sql.parser.validate.FlinkSqlConformance
import org.apache.flink.table.catalog.ObjectPath

/**
 * Flink SQL解析器，用于解析Flink SQL语句中的库、表、分区、操作类型等信息
 *
 * @author ChengLong 2021-6-18 16:41:04
 * @since 2.0.0
 */
object FlinkSqlParser extends SqlParser {
  // calcite parser config
  private lazy val config = createParserConfig
  private lazy val hiveConfig = createHiveParserConfig
  private lazy val tableEnv = FlinkSingletonFactory.getStreamTableEnv
  private lazy val defaultCatalog = this.tableEnv.getCatalog("default_catalog")
  private lazy val hiveCatalog = this.tableEnv.getCatalog(FireHiveConf.hiveCatalogName)

  /**
   * 构建flink default的SqlParser config
   */
  def createParserConfig: CalciteParser.Config = {
    CalciteParser.configBuilder.setParserFactory(
      FlinkSqlParserImpl.FACTORY).
      setQuoting(Quoting.BACK_TICK).
      setUnquotedCasing(Casing.TO_UPPER).
      setQuotedCasing(Casing.UNCHANGED).
      setConformance(FlinkSqlConformance.DEFAULT).
      build
  }

  /**
   * 构建flink hive方言版的SqlParser config
   */
  def createHiveParserConfig: CalciteParser.Config = {
    CalciteParser.configBuilder.setParserFactory(
      FlinkHiveSqlParserImpl.FACTORY).
      setQuoting(Quoting.BACK_TICK).
      setUnquotedCasing(Casing.TO_UPPER).
      setQuotedCasing(Casing.UNCHANGED).
      setConformance(FlinkSqlConformance.DEFAULT).
      build
  }

  /**
   * 根据sql构建Calcite SqlParser
   */
  def parser(sql: String, config: CalciteParser.Config = this.config): SqlNode = {
    CalciteParser.create(sql, config).parseStmt()
  }

  /**
   * 用户解析库表信息以及数据源类型
   */
  private def setTableName(seq: Seq[String], operation: Operation): Unit = {
    val datasource = if (this.isHiveTable(null, seq.head)) Datasource.HIVE else Datasource.VIEW
    if (seq.size == 1) {
      val table = Table("", seq.head, catalog = datasource, operation = operation)
      this.tableMap += (this.tableIdentifier("", seq.head) -> table)
    } else {
      val table = Table(seq.head, seq(1), catalog = datasource, operation = operation)
      this.tableMap += (this.tableIdentifier(seq.head, seq(1)) -> table)
    }
  }

  /**
   * 解析查询SQL中的SqlNode
   */
  @Internal
  private[this] def parseSqlNode(sqlNode: SqlNode, operation: Operation = Operation.SELECT): Unit = {
    sqlNode match {
      case select: SqlSelect => this.parseSqlNode(select.getFrom)
      case sqlJoin: SqlJoin => {
        this.parseSqlNode(sqlJoin.getLeft, operation)
        this.parseSqlNode(sqlJoin.getRight, operation)
      }
      case sqlBasicCall: SqlBasicCall => this.parseSqlNode(sqlBasicCall.operands(0))
      case sqlIdentifier: SqlIdentifier => {
        this.setTableName(sqlIdentifier.names.toSeq, operation)
      }
      case sqlNodeList: SqlNodeList => sqlNodeList.getList.forEach(this.parseSqlNode(_))
      // 解析分区信息
      case sqlProperty: SqlProperty => {
        println(s"分区信息：key=${sqlProperty.getKeyString} value=${sqlProperty.getValueString}")
      }
      // 解析with列表中的属性
      case sqlTableOption: SqlTableOption => {
        println(s"key=${sqlTableOption.getKeyString} value=${sqlTableOption.getValueString}")
      }
      case sqlTableLike: SqlTableLike => this.parseSqlNode(sqlTableLike.getSourceTable)
      case _ => this.logger.warn("不支持的SqlNode")
    }
  }

  /**
   * 用于解析给定的SQL语句
   */
  override def sqlParser(sql: String): Unit = {
    try {
      this.parser(sql) match {
        case select: SqlSelect => this.parseSqlNode(select)
        case insert: RichSqlInsert => {
          this.parseSqlNode(insert.getTargetTable, Operation.INSERT_INTO)
          this.parseSqlNode(insert.getStaticPartitions, Operation.INSERT_INTO)
          this.parseSqlNode(insert.getSource, Operation.SELECT)
        }
        case createView: SqlCreateView => {
          this.parseSqlNode(createView.getViewName, Operation.CREATE_VIEW)
          this.parseSqlNode(createView.getQuery, Operation.SELECT)
        }
        case createTable: SqlCreateTable => {
          this.parseSqlNode(createTable.getTableName, Operation.CREATE_TABLE)
          val tableLike = createTable.getTableLike
          if (tableLike.isPresent) this.parseSqlNode(tableLike.get(), Operation.SELECT) else createTable.getPropertyList.forEach(this.parseSqlNode(_))
        }
        case _ => this.hiveSqlParser(sql)
      }
    } catch {
      case _: Throwable => this.hiveSqlParser(sql)
    }
  }

  /**
   * 用于解析Hive SQL
   */
  def hiveSqlParser(sql: String): Unit = {
    this.parser(sql, this.hiveConfig) match {
      case sqlAddPartitions: SqlAddPartitions => {
        this.parseSqlNode(sqlAddPartitions.getTableName, Operation.ALTER_TABLE_ADD_PARTITION)
        sqlAddPartitions.getPartSpecs.forEach(this.parseSqlNode(_, Operation.ALTER_TABLE_ADD_PARTITION))
      }
      case sqlCreateDatabase: SqlCreateDatabase => {
        this.parseSqlNode(sqlCreateDatabase.getDatabaseName, Operation.CREATE_DATABASE)
      }
      case sqlDropDatabase: SqlDropDatabase => {
        this.parseSqlNode(sqlDropDatabase.getDatabaseName, Operation.DROP_DATABASE)
      }
      case sqlDropPartitions: SqlDropPartitions => {
        this.parseSqlNode(sqlDropPartitions.getTableName, Operation.ALTER_TABLE_DROP_PARTITION)
        sqlDropPartitions.getPartSpecs.forEach(this.parseSqlNode(_, Operation.ALTER_TABLE_DROP_PARTITION))
      }
      case sqlDropTable: SqlDropTable => {
        this.parseSqlNode(sqlDropTable.getTableName, Operation.DROP_TABLE)
      }
      case sqlAlterTableRename: SqlAlterTableRename => {
        this.parseSqlNode(sqlAlterTableRename.getTableName, Operation.RENAME_TABLE_OLD)
      }
      case sqlAlterTable: SqlAlterTable => {
        this.parseSqlNode(sqlAlterTable.getTableName, Operation.ALTER_TABLE)
      }
      case _ => println("未匹配")
    }
  }

  /**
   * 用于判断给定的表是否为临时表
   */
  override def isTempView(dbName: String, tableName: String): Boolean = {
    try {
      if (this.defaultCatalog.isPresent) {
        val catalog = this.defaultCatalog.get()
        val db = if (isEmpty(dbName)) catalog.getDefaultDatabase else dbName
        catalog.tableExists(new ObjectPath(db, tableName))
      } else {
        false
      }
    } catch {
      case e => {
        e.printStackTrace()
        false
      }
    }
  }

  /**
   * 用于判断给定的表是否为hive表
   */
  override def isHiveTable(dbName: String, tableName: String): Boolean = {
    val tableIdentifier = s"$dbName.$tableName"

    if (!this.hiveTableMap.contains(tableIdentifier)) {
      // 根据catalog判断是否为临时表
      if (this.isTempView(dbName, tableName)) {
        this.hiveTableMap.put(tableIdentifier, false)
      } else {
        // 非临时表，进行重量级的解析，依据storage的存储路径进行判断是否为hive表
        if (!this.hiveCatalog.isPresent) {
          this.hiveTableMap.put(tableIdentifier, false)
        } else {
          val catalog = hiveCatalog.get()
          val db = if (isEmpty(dbName)) catalog.getDefaultDatabase else dbName
          try {
            if (catalog.tableExists(new ObjectPath(db, tableName))) {
              this.hiveTableMap.put(tableIdentifier, true)
            } else {
              this.hiveTableMap.put(tableIdentifier, false)
            }
          }
        }
      }
    }

    this.hiveTableMap(tableIdentifier)
  }
}
