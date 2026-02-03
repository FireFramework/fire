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
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.Operation
import com.zto.fire.common.lineage.{LineageManager, SQLLineageManager}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}

import scala.collection.mutable.ArrayBuffer

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
    logicalPlan.children.foreach(child => {
      this.queryParser(child, sinkTable)
      var sourceTable: Option[TableIdentifier] = None
      child match {
        case unresolvedRelation: UnresolvedRelation =>
          this.addCatalog(unresolvedRelation.multipartIdentifier, Operation.SELECT)
          sourceTable = Some(toTableIdentifier(unresolvedRelation.multipartIdentifier))
          // 如果是insert xxx select或create xxx select语句，则维护表与表之间的关系
          if (sinkTable.isDefined) SQLLineageManager.addRelation(toTableIdentifier(unresolvedRelation.multipartIdentifier), sinkTable.get)
        case _ => LineageManager.printLog(s"Parse query SQL异常，无法匹配该Statement. $child")
      }
    })
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
      case dropTable: DropTableCommand =>
        this.addCatalog(this.toFireTableIdentifier(dropTable.tableName), Operation.DROP_TABLE)
      // rename table语句解析
      case renameTableEvent: AlterTableRenameCommand =>
        val tableIdentifier = toFireTableIdentifier(renameTableEvent.oldName)
        val newTableIdentifier = toFireTableIdentifier(renameTableEvent.newName)
        this.addCatalog(tableIdentifier, Operation.RENAME_TABLE_OLD)
        this.addCatalog(newTableIdentifier, Operation.RENAME_TABLE_NEW)
        SQLLineageManager.addRelation(tableIdentifier, newTableIdentifier)
      // rename partition语句解析
      case renamePartition: AlterTableRenamePartitionCommand => {
        val tableIdentifier = this.toFireTableIdentifier(renamePartition.tableName)
        this.addCatalog(tableIdentifier, Operation.RENAME_PARTITION_OLD)
        this.addCatalog(tableIdentifier, Operation.RENAME_PARTITION_NEW)
        SQLLineageManager.setPartitions(tableIdentifier, renamePartition.oldPartition.toSeq)
        SQLLineageManager.setPartitions(tableIdentifier, renamePartition.newPartition.toSeq)
      }
      // drop partition语句解析
      case dropPartition: AlterTableDropPartitionCommand => {
        val tableIdentifier = this.toFireTableIdentifier(dropPartition.tableName)
        this.addCatalog(tableIdentifier, Operation.DROP_PARTITION)
        SQLLineageManager.setPartitions(tableIdentifier, dropPartition.specs.head.toSeq)
      }
      // add partition语句解析
      case addPartition: AlterTableAddPartitionCommand => {
        val tableIdentifier = this.toFireTableIdentifier(addPartition.tableName)
        this.addCatalog(tableIdentifier, Operation.ADD_PARTITION)
        SQLLineageManager.setPartitions(tableIdentifier, addPartition.partitionSpecsAndLocs.head._1.toSeq)
      }
      // truncate table语句解析
      case truncateTable: TruncateTableCommand => {
        val tableIdentifier = this.toFireTableIdentifier(truncateTable.tableName)
        this.addCatalog(tableIdentifier, Operation.TRUNCATE)
      }
      // create view语句解析
      case createView: CreateViewCommand => {
        val identifier = toFireTableIdentifier(createView.name)
        this.addCatalog(identifier, Operation.CREATE_VIEW)
        sinkTable = Some(identifier)
        // 解析视图依赖的表，建立视图与源表之间的血缘关系
        if (createView.plan != null) {
          this.queryParser(createView.plan, sinkTable)
        }
        // 采集视图的列信息
        if (createView.plan != null && createView.plan.output.nonEmpty) {
          val columns = createView.plan.output.map(t => (t.name, t.dataType.toString))
          SQLLineageManager.setColumns(identifier, columns)
        }
      }
      // cache table语句解析
      // 注意：在Spark 3.3中，CacheTableCommand可能不存在于LogicalPlan中，需要通过反射或物理执行计划处理
      case cacheTable: CacheTable => {
        try {
          // 尝试通过反射获取tableIdent字段
          val identifier = cacheTable.multipartIdentifier
          val tableIdentifier = this.toTableIdentifier(identifier)
          this.addCatalog(tableIdentifier, Operation.CACHE)
        } catch {
          case e: Exception =>
            if (FireFrameworkConf.lineageDebugEnable) {
              LineageManager.printLog(s"解析CacheTable失败: ${e.getMessage}, 类名: ${cacheTable.getClass.getName}")
            }
        }
      }
      // uncache table语句解析
      case uncacheTable: UncacheTable => {
        try {
          val tableIdentifier = this.getIdentifier(uncacheTable.table)
          this.addCatalog(tableIdentifier, Operation.UNCACHE)
        } catch {
          case e: Exception =>
            if (FireFrameworkConf.lineageDebugEnable) {
              LineageManager.printLog(s"解析UncacheTable失败: ${e.getMessage}, 类名: ${uncacheTable.getClass.getName}")
            }
        }
      }
      case _ => LineageManager.printLog(s"Parse ddl SQL异常，无法匹配该Statement. $logicalPlan")
    }
    sinkTable
  }

  /**
   * 用于解析DDL语句中的库表、分区信息
   *
   * @return 返回sink目标表，用于维护表与表之间的关系
   */
  override def ddlParserWithPlan(sparkPlan: SparkPlan): Option[TableIdentifier] = {
    var sinkTable: Option[TableIdentifier] = None
    LineageManager.printLog(s"开始解析物理执行计划, $sparkPlan")
    sparkPlan.collect {
      // Hive表扫描信息
      case plan if plan.getClass.getName == "org.apache.spark.sql.hive.execution.HiveTableScanExec" =>
        val relationField = plan.getClass.getDeclaredField("relation")
        relationField.setAccessible(true)
        val relation = relationField.get(plan).asInstanceOf[HiveTableRelation]
        val tableIdentifier = this.toFireTableIdentifier(relation.tableMeta.identifier)
        LineageManager.printLog(s"hive scan解析到select表名: $tableIdentifier")
        this.addCatalog(tableIdentifier, Operation.SELECT)
        sinkTable = Some(tableIdentifier)
      // cache scan
      case p: InMemoryTableScanExec =>
        handleInMemoryTableScan(p).foreach(x => {
          LineageManager.printLog(s"cache scan中解析到select表名: $x")
          this.addCatalog(x, Operation.SELECT)
          sinkTable = Some(x)
        })
      // 表写入信息
      case plan: DataWritingCommandExec =>
        plan.cmd match {
          case CreateDataSourceTableAsSelectCommand(table, mode, query, outputColumnNames) =>
            val tableIdentifier = this.toFireTableIdentifier(table.identifier)
            this.addCatalog(tableIdentifier, Operation.CREATE_TABLE)
            sinkTable = Some(tableIdentifier)
          case CreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode) =>
            val tableIdentifier = this.toFireTableIdentifier(tableDesc.identifier)
            this.addCatalog(tableIdentifier, Operation.CREATE_TABLE)
            sinkTable = Some(tableIdentifier)
          case InsertIntoHadoopFsRelationCommand(outputPath, staticPartitions, ifPartitionNotExists, partitionColumns, bucketSpec, fileFormat, options, query, mode, catalogTable, fileIndex, outputColumnNames) =>
          case InsertIntoHiveTable(table, partition, query, overwrite, ifPartitionNotExists, outputColumnNames) => {
            val tableIdentifier = this.toFireTableIdentifier(table.identifier)
            this.addCatalog(tableIdentifier, Operation.INSERT_INTO)
            sinkTable = Some(tableIdentifier)
          }
          case InsertIntoHiveDirCommand(isLocal, storage, query, overwrite, outputColumnNames) =>
          /*case DeleteFromTableExec(table, condition, refreshCache) => {
            val tableIdentifier = TableIdentifier(table.name())
            this.addCatalog(tableIdentifier, Operation.DELETE)
            sinkTable = Some(tableIdentifier)
          }*/
        }
      // 命令
      case plan: ExecutedCommandExec => plan.cmd match {
        case AlterDatabasePropertiesCommand(databaseName, props) =>
        case AlterTableAddColumnsCommand(tableName, colsToAdd) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableChangeColumnCommand(tableName, columnName, newColumn) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableRenameCommand(oldName, newName, isView) =>
          val oldTableIdentifier = this.toFireTableIdentifier(oldName)
          this.addCatalog(oldTableIdentifier, Operation.ALTER_TABLE)
          val tableIdentifier = this.toFireTableIdentifier(newName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableRenamePartitionCommand(tableName, oldPartition, newPartition) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableSerDePropertiesCommand(tableName, serdeClassName, serdeProperties, partSpec) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableSetLocationCommand(tableName, partitionSpec, location) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableSetPropertiesCommand(tableName, properties, isView) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AlterTableUnsetPropertiesCommand(tableName, propKeys, ifExists, isView) =>
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.ALTER_TABLE)
        case AnalyzePartitionCommand(tableIdent, partitionSpec, noscan) => {
          val tableIdentifier = this.toFireTableIdentifier(tableIdent)
          this.addCatalog(tableIdentifier, Operation.ANALYZE_PARTITION)
        }
        case AnalyzeTableCommand(tableIdent, noscan) => {
          val tableIdentifier = this.toFireTableIdentifier(tableIdent)
          this.addCatalog(tableIdentifier, Operation.ANALYZE_TABLE)
        }
        case CreateDataSourceTableCommand(table, ignoreIfExists) =>
        case CreateDatabaseCommand(databaseName, ifNotExists, path, comment, props) => {
          this.addCatalog(TableIdentifier("", databaseName), Operation.CREATE_DATABASE)
        }
        case CreateFunctionCommand(databaseName, functionName, className, resources, isTemp, ignoreIfExists, replace) => {
          this.addCatalog(TableIdentifier("", databaseName.getOrElse("")), Operation.CREATE_DATABASE)
        }
        case CreateTableCommand(table, ignoreIfExists) => {
          val tableIdentifier = this.toFireTableIdentifier(table.identifier)
          this.addCatalog(tableIdentifier, Operation.CREATE_TABLE)
        }
        case CreateTableLikeCommand(targetTable, sourceTable, fileFormat, provider, properties, ifNotExists) => {
          val sourceIdentifier = this.toFireTableIdentifier(sourceTable)
          this.addCatalog(sourceIdentifier, Operation.SELECT)
          val targetIdentifier = this.toFireTableIdentifier(targetTable)
          this.addCatalog(targetIdentifier, Operation.CREATE_TABLE_LIKE)
        }
        case CreateTempViewUsing(tableIdent, userSpecifiedSchema, replace, global, provider, options) =>  {
          val tableIdentifier = this.toFireTableIdentifier(tableIdent)
          this.addCatalog(tableIdentifier, Operation.CREATE_VIEW)
        }
        case CreateViewCommand(name, userSpecifiedColumns, comment, properties, originalText, plan, allowExisting, replace, viewType, isAnalyzed, referredTempFunctions) => {
          val tableIdentifier = this.toFireTableIdentifier(name)
          this.addCatalog(tableIdentifier, Operation.CREATE_VIEW)
        }
        case DescribeColumnCommand(tableName, colNameParts, isExtended, output) =>
        case DescribeDatabaseCommand(databaseName, extended, output) =>
        case DescribeFunctionCommand(functionName, isExtended) =>
        case DescribeTableCommand(tableName, partitionSpec, isExtended, output) =>
        case DropDatabaseCommand(databaseName, ifExists, cascade) =>
        case DropFunctionCommand(databaseName, functionName, ifExists, isTemp) =>
        case DropTableCommand(tableName, ifExists, isView, purge) => {
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.DROP_TABLE)
        }
        case ExplainCommand(logicalPlan, mode) =>
        case InsertIntoDataSourceCommand(logicalRelation, query, overwrite) =>
        case InsertIntoDataSourceDirCommand(storage, provider, query, overwrite) =>
        case ListFilesCommand(files) =>
        case ListJarsCommand(jars) =>
        case LoadDataCommand(table, path, isLocal, isOverwrite, partition) =>
        case RefreshResource(path) =>
        case RefreshTableCommand(tableIdent) => {
          val tableIdentifier = this.toFireTableIdentifier(tableIdent)
          this.addCatalog(tableIdentifier, Operation.REFRESH)
        }
        case ResetCommand(config) =>
        case SaveIntoDataSourceCommand(query, dataSource, options, mode) =>
        case SetCommand(kv) =>
        case ShowColumnsCommand(databaseName, tableName, output) =>
        case ShowCreateTableCommand(tableName, output) => {
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.SHOW_CREATE_TABLE)
        }
        case ShowFunctionsCommand(db, pattern, showUserFunctions, showSystemFunctions, output) =>
        case ShowPartitionsCommand(tableName, output, spec) => {
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.SHOW_PARTITION)
        }
        case ShowTablePropertiesCommand(tableName, propertyKey, output) => {
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.SHOW_TABLE_PROPERTIES)
        }
        case ShowTablesCommand(databaseName, tableIdentifierPattern, output, isExtended, partitionSpec) =>
        case StreamingExplainCommand(queryExecution, extended) =>
        case TruncateTableCommand(tableName, partitionSpec) => {
          val tableIdentifier = this.toFireTableIdentifier(tableName)
          this.addCatalog(tableIdentifier, Operation.TRUNCATE)
        }
        // Cache/Uncache命令处理（在物理执行计划中）
        // 在Spark 3.3中，CacheTableCommand和UncacheTableCommand可能不存在，使用反射方式处理
        case cmd if cmd.getClass.getSimpleName.contains("CacheTable") => {
          try {
            val tableIdentField = cmd.getClass.getDeclaredField("tableIdent")
            tableIdentField.setAccessible(true)
            val tableIdent = tableIdentField.get(cmd)
            val tableIdentifier = this.toFireTableIdentifier(tableIdent.asInstanceOf[org.apache.spark.sql.catalyst.TableIdentifier])
            this.addCatalog(tableIdentifier, Operation.CACHE)
          } catch {
            case e: Exception =>
              if (FireFrameworkConf.lineageDebugEnable) {
                LineageManager.printLog(s"解析物理执行计划中CacheTable失败: ${e.getMessage}, 类名: ${cmd.getClass.getName}")
              }
          }
        }
        case cmd if cmd.getClass.getSimpleName.contains("UncacheTable") => {
          try {
            val tableIdentField = cmd.getClass.getDeclaredField("tableIdent")
            tableIdentField.setAccessible(true)
            val tableIdent = tableIdentField.get(cmd)
            val tableIdentifier = this.toFireTableIdentifier(tableIdent.asInstanceOf[org.apache.spark.sql.catalyst.TableIdentifier])
            this.addCatalog(tableIdentifier, Operation.UNCACHE)
          } catch {
            case e: Exception =>
              if (FireFrameworkConf.lineageDebugEnable) {
                LineageManager.printLog(s"解析物理执行计划中UncacheTable失败: ${e.getMessage}, 类名: ${cmd.getClass.getName}")
              }
          }
        }
        case _ => LineageManager.printLog(s"解析物理执行计划异常，无法匹配该Statement")
      }
    }
    sinkTable
  }

  /**
   * 处理执行计划中InMemoryTableScanExec
   *
   * @param plan 物理执行计划
   * @return 表操作信息
   */
  def handleInMemoryTableScan(plan: SparkPlan): Seq[TableIdentifier] = {
    plan match {
      case p if p.getClass.getName == "org.apache.spark.sql.hive.execution.HiveTableScanExec" =>
        val relationField = p.getClass.getDeclaredField("relation")
        relationField.setAccessible(true)
        val relation = relationField.get(plan).asInstanceOf[HiveTableRelation]
        val tableIdentifier = this.toFireTableIdentifier(relation.tableMeta.identifier)
        Seq(tableIdentifier)
      // case p: QueryStageInput => handleInMemoryTableScan(p.childStage)
      case p: QueryStageExec => handleInMemoryTableScan(p.plan)
      case p: InMemoryTableScanExec =>
        // 尝试从InMemoryRelation中提取表名
        try {
          val relation = p.relation
          // 尝试通过反射获取tableName字段（InMemoryRelation可能包含表名信息）
          try {
            val tableNameField = relation.getClass.getDeclaredField("tableName")
            tableNameField.setAccessible(true)
            val tableNameOpt = tableNameField.get(relation).asInstanceOf[Option[String]]
            if (tableNameOpt.isDefined) {
              val tableIdentifier = this.toFireTableIdentifier(org.apache.spark.sql.catalyst.TableIdentifier(tableNameOpt.get))
              return Seq(tableIdentifier)
            }
          } catch {
            case _: NoSuchFieldException =>
              // tableName字段不存在，继续尝试其他方式
          }
          // 如果无法从relation中直接获取表名，递归处理cachedPlan
          handleInMemoryTableScan(p.relation.cachedPlan)
        } catch {
          case e: Exception =>
            if (FireFrameworkConf.lineageDebugEnable) {
              LineageManager.printLog(s"解析InMemoryTableScanExec失败: ${e.getMessage}, 类名: ${plan.getClass.getName}")
            }
            // 如果提取失败，递归处理cachedPlan
            try {
              handleInMemoryTableScan(p.relation.cachedPlan)
            } catch {
              case _: Exception => Seq.empty
            }
        }
      case p: SparkPlan => p.children.flatMap(handleInMemoryTableScan)
    }
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
