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

package com.zto.fire.spark.sql

import com.zto.fire._
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.enu.Operation
import com.zto.fire.common.lineage.SQLLineageManager
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.CreateTable

/**
 * Spark SQL解析器，用于解析Spark SQL语句中的库、表、分区、操作类型等信息
 *
 * @author ChengLong 2021-6-18 16:31:04
 * @since 2.0.0
 */
@Internal
private[fire] object SparkSqlParser extends SparkSqlParserBase {

  /**
   * 用于解析查询sql中的库表信息
   *
   * @param sinkTable
   * 当insert xxx select或create xxx select语句时，sinkTable不为空
   */
  override def queryParser(logicalPlan: LogicalPlan, sinkTable: Option[TableIdentifier]): Unit = {
    logicalPlan match {
      case statement: CreateViewStatement =>
        this.queryParser(statement.child, sinkTable)
      case _ => {
        logicalPlan.children.foreach(child => {
          this.queryParser(child, sinkTable)
          var sourceTable: Option[TableIdentifier] = None
          child match {
            case unresolvedRelation: UnresolvedRelation =>
              this.addCatalog(unresolvedRelation.multipartIdentifier, Operation.SELECT)
              sourceTable = Some(toTableIdentifier(unresolvedRelation.multipartIdentifier))
              // 如果是insert xxx select或create xxx select语句，则维护表与表之间的关系
              if (sinkTable.isDefined) SQLLineageManager.addRelation(toTableIdentifier(unresolvedRelation.multipartIdentifier), sinkTable.get, null)
            case _ => this.logger.debug(s"Parse query SQL异常，无法匹配该Statement. ")
          }
        })
      }
    }
  }

  /**
   * 用于解析DDL语句中的库表、分区信息
   *
   * @return 返回sink目标表，用于维护表与表之间的关系
   */
  override def ddlParser(logicalPlan: LogicalPlan): Option[TableIdentifier] = {
    var sinkTable: Option[TableIdentifier] = None
    logicalPlan match {
      // insert into语句解析
      case insertInto: InsertIntoStatement => {
        val identifier = insertInto.table.asInstanceOf[UnresolvedRelation].multipartIdentifier
        this.addCatalog(identifier, Operation.INSERT_INTO)
        // 维护分区信息
        val fireTableIdentifier = toTableIdentifier(identifier)
        val partitions = insertInto.partitionSpec.map(part => (part._1, if (part._2.isDefined) part._2.get else ""))
        SQLLineageManager.setPartitions(fireTableIdentifier, partitions.toSeq)
        sinkTable = Some(fireTableIdentifier)
      }
      // drop table语句解析
      case dropTable: DropTableStatement => {
        this.addCatalog(dropTable.tableName, Operation.DROP_TABLE)
      }
      // rename table语句解析
      case renameTable: RenameTableStatement => {
        this.addCatalog(renameTable.oldName, Operation.RENAME_TABLE_OLD)
        this.addCatalog(renameTable.newName, Operation.RENAME_TABLE_NEW)
        SQLLineageManager.addRelation(toTableIdentifier(renameTable.oldName), toTableIdentifier(renameTable.newName), null)
      }
      // create table as select语句解析
      case createTableAsSelect: CreateTableAsSelectStatement => {
        val identifier = this.toTableIdentifier(createTableAsSelect.tableName)
        this.addCatalog(identifier, Operation.CREATE_TABLE_AS_SELECT)
        // 采集建表属性信息
        SQLLineageManager.setOptions(identifier, createTableAsSelect.properties)
        sinkTable = Some(identifier)
      }
      // create table语句解析
      case createTable: CreateTable => {
        val identifier = this.toFireTableIdentifier(createTable.tableDesc.identifier)
        this.addCatalog(identifier, Operation.CREATE_TABLE)
        sinkTable = Some(identifier)
        // 采集建表属性信息
        SQLLineageManager.setOptions(identifier, createTable.tableDesc.properties)
        // 采集分区字段信息
        val partitions = createTable.tableDesc.partitionSchema.map(st => (st.dataType.toString, st.name))
        SQLLineageManager.setPartitions(identifier, partitions)
      }
      case createTable: CreateTableStatement => {
        val identifier = this.toTableIdentifier(createTable.tableName)
        this.addCatalog(identifier, Operation.CREATE_TABLE)
        sinkTable = Some(identifier)
        // 采集建表属性信息
        SQLLineageManager.setOptions(identifier, createTable.options)
        // 采集分区字段信息
        val partitions = createTable.partitioning.map(st => (st.toString, st.name))
        SQLLineageManager.setPartitions(identifier, partitions)
      }
      case createView: CreateViewCommand => {
        val identifier = toFireTableIdentifier(createView.name)
        this.addCatalog(identifier, Operation.CREATE_VIEW)
        SQLLineageManager.setColumns(identifier, createView.child.output.map(t => (t.name, t.dataType.toString)))
      }
      case createView: CreateViewStatement => {
        val identifier = this.toTableIdentifier(createView.viewName)
        this.addCatalog(identifier, Operation.CREATE_VIEW_AS_SELECT)
        // 采集建表属性信息
        sinkTable = Some(identifier)
      }
      // rename partition语句解析
      case renamePartition: AlterTableRenamePartitionStatement => {
        this.addCatalog(renamePartition.tableName, Operation.RENAME_PARTITION_OLD)
        this.addCatalog(renamePartition.tableName, Operation.RENAME_PARTITION_NEW)
        SQLLineageManager.setPartitions(this.toTableIdentifier(renamePartition.tableName), renamePartition.from.toSeq)
        SQLLineageManager.setPartitions(this.toTableIdentifier(renamePartition.tableName), renamePartition.to.toSeq)
      }
      // drop partition语句解析
      case dropPartition: AlterTableDropPartitionStatement => {
        this.addCatalog(dropPartition.tableName, Operation.DROP_PARTITION)
        SQLLineageManager.setPartitions(this.toTableIdentifier(dropPartition.tableName), dropPartition.specs.head.toSeq)
      }
      // add partition语句解析
      case addPartition: AlterTableAddPartitionStatement => {
        this.addCatalog(addPartition.tableName, Operation.ADD_PARTITION)
        SQLLineageManager.setPartitions(this.toTableIdentifier(addPartition.tableName), addPartition.partitionSpecsAndLocs.head._1.toSeq)
      }
      // truncate table语句解析
      case truncateTable: TruncateTableStatement => {
        this.addCatalog(truncateTable.tableName, Operation.TRUNCATE)
      }
      case cacheTable: CacheTableStatement => {
        this.addCatalog(cacheTable.tableName, Operation.CACHE)
      }
      case uncacheTable: UncacheTableStatement => {
        this.addCatalog(uncacheTable.tableName, Operation.UNCACHE)
      }
      case refreshTable: RefreshTableStatement => {
        this.addCatalog(refreshTable.tableName, Operation.REFRESH)
      }
      case deleteFromTable: DeleteFromTable => {
        val tableIdentifier = getIdentifier(deleteFromTable.table)
        this.addCatalog(tableIdentifier, Operation.DELETE)
      }
      case updateTable: UpdateTable => {
        val tableIdentifier = getIdentifier(updateTable.table)
        this.addCatalog(tableIdentifier, Operation.UPDATE)
      }
      case mergeIntoTable: MergeIntoTable => {
        val tableIdentifier = getIdentifier(mergeIntoTable.targetTable)
        this.addCatalog(tableIdentifier, Operation.MERGE)
      }
      case _ => this.logger.debug(s"Parse ddl SQL异常，无法匹配该Statement.")
    }

    sinkTable
  }

  /**
   * 用于判断给定的表是否为临时表
   */
  @Internal
  override def isTempView(tableIdentifier: TableIdentifier): Boolean = {
    tryWithReturn {
      catalog.isTempView(tableIdentifier.toNameParts)
    }(this.logger, catchLog = s"判断${tableIdentifier}是否为临时表或视图失败", hook = false)
  }
}
