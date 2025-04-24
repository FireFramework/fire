/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.common.util

import com.zto.fire.common.enu.{Operation, SqlSemantic}
import com.zto.fire.common.lineage.parser.connector.MongodbConnectorParser
import com.zto.fire.predef._
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.relational.{EqualsTo, ExpressionList}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.alter.{Alter, RenameTableStatement}
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.merge.Merge
import net.sf.jsqlparser.statement.replace.Replace
import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.statement.truncate.Truncate
import net.sf.jsqlparser.statement.update.Update
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
 * SQL相关工具类
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-26 15:09
 */
object SQLUtils extends Logging {
  private[this] val beforeWorld = "(?i)(from|join|into table|^\\s*update|table|into|exists|desc|like|if)"
  private[this] val reg = s"${beforeWorld}\\s+(\\w+\\.\\w+|\\w+)".r
  private[this] val passwordReg = "'password'\\s*=\\s*'.+'"

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

  /**
   * 使用正则表达式拆分以分号为分隔符的字符串
   */
  def splitSql(sql: String): Seq[String] = {
    require(StringUtils.isNotBlank(sql), "待分割的sql语句不能为空")
    sql.split(RegularUtils.sqlSplit)
  }

  /**
   * 执行多条sql语句，以分号分割
   */
  def executeSql[T](sql: String)(block: String => T): Option[T] = {
    var result: Option[T] = None
    this.splitSql(sql).filter(noEmpty(_)).foreach(statement => {
      if (noEmpty(statement)) {
        logDebug("当前执行sql：\n" + statement)
        result = Some(block(statement))
      }
    })
    result
  }

  /**
   * 判断给定SQL的语义：SELECT、DROP、CREATE、INSERT、ALTER等
   */
  def sqlSemantic(sql: String): SqlSemantic = {
    if (isEmpty(sql)) return SqlSemantic.UNKNOWN
    val finalSql = StringUtils.trim(sql).toUpperCase

    SqlSemantic.values().foreach(semantic => {
      if (finalSql.startsWith(semantic.toString)) return semantic
    })

    SqlSemantic.UNKNOWN
  }

  /**
   * 替换SQL中的敏感信息
   *
   * @param sql
   * 带有敏感信息的SQL语句
   * @return
   * 替换敏感信息后的SQL语句
   */
  def hideSensitive(sql: String): String = {
    val sensitiveSql = sql.replaceAll(passwordReg, s"'password'='${RegularUtils.hidePassword}'")
    val tmpSQL = MongodbConnectorParser.hideSensitive(sensitiveSql)
    tmpSQL
  }

  /**
   * 用于解析dml语句中占位符对应的字段列表
   *
   * @return
   * dml语句中字段的列表
   */
  def parsePlaceholder(sql: String): List[String] = {
    val columns = new ListBuffer[Column]()

    /**
     * 根据不同的dml语句类型进行单独的解析
     */
    CCJSqlParserUtil.parse(sql) match {
      case i: Insert => this.parseInsertStatement(columns, i)
      case u: Update => this.parseUpdateStatement(columns, u)
      case d: Delete => this.parseExpression(d.getWhere, columns)
      case m: Merge => this.parseMergeStatement(getPreparedColumns _, columns, m)
      case r: Replace => this.parseReplaceStatement(columns, r)
      case _ =>
    }

    columns.map(_.getColumnName).toList
  }

  /**
   * 用于解析SQL语句中的血缘信息
   *
   * @return
   * dml语句中字段的列表
   */
  def parseLineage(sql: String): List[(String, Operation)] = {
    val lineages = new ListBuffer[(String, Operation)]()

    tryWithLog {
      CCJSqlParserUtil.parse(sql) match {
        case s: Select => lineages ++= this.parseSelectLineage(s)
        case a: Alter => lineages ++= this.parseAlterLineage(a)
        case c: CreateTable => lineages ++= this.parseCreateTableLineage(c)
        case d: Drop => lineages ++= this.parseDropLineage(d)
        case t: Truncate => lineages ++= this.parseTruncateLineage(t)
        case r: RenameTableStatement => lineages ++= this.parseRenameTableLineage(r)
        case i: Insert => lineages ++= this.parseInsertLineage(i)
        case r: Replace => lineages ++= this.parseReplaceLineage(r)
        case u: Update => lineages ++= this.parseUpdateLineage(u)
        case d: Delete => lineages ++= this.parseDeleteLineage(d)
        case m: Merge => lineages ++= this.parseMergeLineage(m)
        case _ => this.logWarning(s"发现未能解析血缘的JDBC SQL语句：$sql")
      }
    } (this.logger, catchLog = s"JDBC sql血缘解析失败：$sql")

    lineages.filter(t => noEmpty(t._1)).toList
  }

  /**
   * 解析merge语句中的血缘信息
   */
  private[this] def parseMergeLineage(m: Merge): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(m.getTable), Operation.MERGE))
    if (m.getUsingSelect != null) {
      this.parseSelectBodyLineage(m.getUsingSelect.getSelectBody, tables)
    }

    if (m.getUsingTable != null) {
      tables.add((this.getTableName(m.getUsingTable), Operation.SELECT))
    }

    tables
  }

  /**
   * 解析delete语句中的血缘信息
   */
  private[this] def parseDeleteLineage(d: Delete): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(d.getTable), Operation.DELETE))
    tables
  }

  /**
   * 解析update语句中的血缘信息
   */
  private[this] def parseUpdateLineage(u: Update): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(u.getTable), Operation.UPDATE))
    tables.addAll(this.parseSelectLineage(u.getSelect))
    tables
  }

  /**
   * 解析replace语句中的血缘信息
   */
  private[this] def parseReplaceLineage(r: Replace): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(r.getTable), Operation.REPLACE_INTO))
    if (r.getItemsList != null && r.getItemsList.isInstanceOf[SubSelect]) {
      this.parseSelectBodyLineage(r.getItemsList.asInstanceOf[SubSelect].getSelectBody, tables)
    }

    tables
  }

  /**
   * 解析insert语句中的血缘信息
   */
  private[this] def parseInsertLineage(i: Insert): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(i.getTable), Operation.INSERT))
    tables.addAll(this.parseSelectLineage(i.getSelect))
    tables
  }

  /**
   * 解析rename table语句中的血缘信息
   */
  private[this] def parseRenameTableLineage(r: RenameTableStatement): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    r.getTableNames.foreach(t => {
      tables.add((this.getTableName(t.getKey), Operation.RENAME_TABLE_OLD))
      tables.add((this.getTableName(t.getValue), Operation.RENAME_TABLE_NEW))
    })
    tables
  }

  /**
   * 解析Truncate语句中的血缘信息
   */
  private[this] def parseTruncateLineage(t: Truncate): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(t.getTable), Operation.TRUNCATE))
    tables
  }

  /**
   * 解析Alter语句中的血缘信息
   */
  private[this] def parseDropLineage(d: Drop): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(d.getName), Operation.DROP_TABLE))
    tables
  }

  /**
   * 解析Alter语句中的血缘信息
   */
  private[this] def parseCreateTableLineage(c: CreateTable): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(c.getTable), Operation.CREATE_TABLE))
    tables
  }

  /**
   * 获取表名
   */
  private[this] def getTableName(table: Table): String = {
    if (table == null) return ""
    if (isEmpty(table.getName)) return ""

    val tableName = if (noEmpty(table.getDatabase, table.getDatabase.toString)) s"${table.getDatabase.getDatabaseName}.${table.getName}" else table.getName
    tableName.replaceAll("`", "")
  }

  /**
   * 解析Alter语句中的血缘信息
   */
  private[this] def parseAlterLineage(a: Alter): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(String, Operation)]()
    tables.add((this.getTableName(a.getTable), Operation.ALTER_TABLE))
    tables
  }

  /**
   * 解析select语句中的血缘信息
   */
  private[this] def parseSelectLineage(s: Select): JHashSet[(String, Operation)] = {
    val tables = new JHashSet[(JString, Operation)]()
    if (s == null) return tables
    this.parseSelectBodyLineage(s.getSelectBody, tables)

    tables
  }

  /**
   * 解析SelectBody结构中的血缘信息
   * with语法暂不支持解析
   */
  private def parseSelectBodyLineage(body: SelectBody, tables: JHashSet[(JString, Operation)]): Unit = {
    body match {
      case plainSelect: PlainSelect =>
        val fromItem = plainSelect.getFromItem
        processFromItem(fromItem, tables)

        val joins = plainSelect.getJoins
        if (joins != null && joins.nonEmpty) {
          joins.filter(t => t != null && t.getRightItem != null).foreach(join => {
            processFromItem(join.getRightItem, tables)
          })
        }
      case otherItem => this.logWarning(s"子查询暂不支持解析: ${otherItem.getClass.getName}")
    }
  }

  /**
   * 递归解析子查询中的血缘信息
   */
  private def processFromItem(item: FromItem, tables: JHashSet[(JString, Operation)]): Unit = item match {
    case table: Table =>
      tables.add((getTableName(table), Operation.SELECT))
    case subSelect: SubSelect =>
      parseSelectBodyLineage(subSelect.getSelectBody, tables)
    case otherItem => this.logWarning(s"Item暂不支持解析: ${otherItem.getClass.getName}")
  }

  /**
   * 只解析使用占位符的字段
   */
  private[this] def getPreparedColumns(columns: JList[Column], values: JList[Expression]): JList[Column] = {
    val list = new JLinkedList[Column]()

    if (noEmpty(columns, values) && values.size() > 0) {
      for (i <- 0 until columns.size()) {
        if (i < values.size && "?".equals(values.get(i).toString.trim)) {
          list.add(columns.get(i))
        }
      }
    }
    list
  }

  /**
   * 解析replace语句中的字段列表
   */
  private[this] def parseReplaceStatement(columns: ListBuffer[Column], r: Replace): Unit = {
    if (noEmpty(r.getColumns, r.getItemsList)) {
      val exp = r.getItemsList.asInstanceOf[ExpressionList].getExpressions
      if (exp.size() == r.getColumns.size()) {
        for (i <- 0 until r.getColumns.size()) {
          if (i < exp.size()) {
            val placeholder = exp.get(i).toString.trim
            if ("?".equals(placeholder)) {
              columns.add(r.getColumns.get(i))
            }
          }
        }
      }
    }
  }

  /**
   * 解析merge语句中的字段列表
   */
  private[this] def parseMergeStatement(getPreparedColumns: (JList[Column], JList[Expression]) => JList[Column], columns: ListBuffer[Column], m: Merge): Unit = {
    val select = m.getUsingSelect
    if (select != null && select.getSelectBody != null && select.getSelectBody.isInstanceOf[PlainSelect]) {
      select.getSelectBody.asInstanceOf[PlainSelect].getSelectItems.foreach(item => {
        if (item.toString.contains("?") && item.isInstanceOf[SelectExpressionItem]) {
          val each = item.asInstanceOf[SelectExpressionItem]
          columns.add(new Column(each.getAlias.getName))
        }
      })
    }
    val update = m.getMergeUpdate
    if (update != null) columns ++= getPreparedColumns(update.getColumns, update.getValues)
    val merge = m.getMergeInsert
    if (merge != null) columns ++= getPreparedColumns(merge.getColumns, merge.getValues)
  }

  /**
   * 解析update语句中的字段列表
   */
  private[this] def parseUpdateStatement(columns: ListBuffer[Column], u: Update): Unit = {
    columns ++= u.getUpdateSets.flatMap(t => t.getColumns).filter(column => {
      val result = this.getColumnValue(column)
      "?".equals(result._2.trim)
    })
    this.parseExpression(u.getWhere, columns)
  }

  /**
   * 解析insert语句中的字段列表
   */
  private[this] def parseInsertStatement(columns: ListBuffer[Column], i: Insert): Unit = {
    if (noEmpty(i.getSelect, i.getSelect.getSelectBody)) {
      // 获取占位符数量
      val placeholderCount = i.getSelect.getSelectBody.toString.count(_ == '?')
      // 只截取与占位符数量相同的字段列表，由于顺序的不确定性，约定占位符的字段放在最左侧声明
      columns ++= i.getColumns.take(placeholderCount)

      if (i.isUseDuplicate && noEmpty(i.getDuplicateUpdateColumns)) {
        // 解析DUPLICATE KEY中的字段
        columns ++= i.getDuplicateUpdateColumns.filter(column => {
          val result = this.getColumnValue(column)
          "?".equals(result._2.trim)
        })
      }
    }
  }

  /**
   * 递归解析表达式中的字段列表
   */
  private[this] def parseExpression(node: Expression, columns: JList[Column]): Unit = {
    node match {
      case and: AndExpression => {
        parseExpression(and.getLeftExpression, columns)
        parseExpression(and.getRightExpression, columns)
      }
      case equ: EqualsTo => {
        parseExpression(equ.getLeftExpression, columns)
        parseExpression(equ.getRightExpression, columns)
      }
      case column: Column => {
        val result = this.getColumnValue(column)
        if ("?".equals(result._2.trim)) {
          columns.add(result._1)
        }
      }
      case _ =>
    }
  }

  /**
   * 解析占位符对应的列
   *
   * @return
   * (列名、列的值：？)
   */
  private[this] def getColumnValue(column: Column): (Column, String) = {
    val token = column.getASTNode.jjtGetValue().asInstanceOf[Column]
    val value = token.getASTNode.jjtGetFirstToken().next.next.toString
    (token, value)
  }
}
