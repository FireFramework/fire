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
import com.zto.fire.common.lineage.{LineageManager, SQLLineageManager}
import com.zto.fire.spark.util.TiSparkUtils
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, _}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{AlterDatabasePropertiesCommand, AlterTableAddColumnsCommand, AlterTableAddPartitionCommand, AlterTableChangeColumnCommand, AlterTableDropPartitionCommand, AlterTableRenameCommand, AlterTableRenamePartitionCommand, AlterTableSerDePropertiesCommand, AlterTableSetLocationCommand, AlterTableSetPropertiesCommand, AlterTableUnsetPropertiesCommand, AlterViewAsCommand, AnalyzeColumnCommand, AnalyzePartitionCommand, AnalyzeTableCommand, ClearCacheCommand, CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand, CreateDatabaseCommand, CreateFunctionCommand, CreateTableCommand, CreateTableLikeCommand, CreateViewCommand, DataWritingCommandExec, DescribeColumnCommand, DescribeDatabaseCommand, DescribeFunctionCommand, DescribeTableCommand, DropDatabaseCommand, DropFunctionCommand, DropTableCommand, ExecutedCommandExec, ExplainCommand, InsertIntoDataSourceDirCommand, ListFilesCommand, ListJarsCommand, LoadDataCommand, RefreshTableCommand, ResetCommand, SetCommand, ShowColumnsCommand, ShowCreateTableCommand, ShowFunctionsCommand, ShowPartitionsCommand, ShowTablePropertiesCommand, ShowTablesCommand, StreamingExplainCommand, TruncateTableCommand}
import org.apache.spark.sql.execution.datasources.v2.DeleteFromTableExec
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, RefreshResource, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}

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
      // create table as select语句解析
      /*case createTableAsSelect: CreateTableAsSelect => {
        val identifier = createTableAsSelect
        println(identifier)
        val identifier = this.toTableIdentifier(createTableAsSelect.tableName)
        this.addCatalog(identifier, Operation.CREATE_TABLE_AS_SELECT)
        // 采集建表属性信息
        SQLLineageManager.setOptions(identifier, createTableAsSelect.properties)
        sinkTable = Some(identifier)
      }*/
      // TODO:
      // create table语句解析
      /*case createTable: CreateTable => {
        val createTable2 = createTable
        createTable.table
        println(createTable2)
        /*val identifier = this.toFireTableIdentifier(createTable.tableDesc.identifier)
        this.addCatalog(identifier, Operation.CREATE_TABLE)
        sinkTable = Some(identifier)
        // 采集建表属性信息
        SQLLineageManager.setOptions(identifier, createTable.tableDesc.properties)
        // 采集分区字段信息
        val partitions = createTable.tableDesc.partitionSchema.map(st => (st.dataType.toString, st.name))
        SQLLineageManager.setPartitions(identifier, partitions)*/
      }*/
      /*
      case createView: CreateViewCommand => {
        val identifier = toFireTableIdentifier(createView.name)
        this.addCatalog(identifier, Operation.CREATE_VIEW)
        SQLLineageManager.setColumns(identifier, createView.child.output.map(t => (t.name, t.dataType.toString)))
        // 采集建表属性信息
        sinkTable = Some(identifier)
      }
      case createView: CreateViewCommand => {
        val identifier = toFireTableIdentifier(createView.name)
        sinkTable = Some(identifier)
        this.addCatalog(identifier, Operation.CREATE_VIEW)
        SQLLineageManager.setColumns(identifier, createView.child.output.map(t => (t.name, t.dataType.toString)))

        if (logicalPlan.toString().contains("TiDBRelation")) {
          val tableIdentifier = TiSparkUtils.parseTableIdentifier(logicalPlan)
          if (tableIdentifier.isDefined) {
            this.addCatalog(tableIdentifier.get, Operation.SELECT)
            if (sinkTable.isDefined) SQLLineageManager.addRelation(tableIdentifier.get, sinkTable.get, null)
          }
        }
      }*/
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
      // TODO:
      /*case cacheTable: CacheTable => {
        val tableIdentifier = this.toFireTableIdentifier(cacheTable.tableIdent)
        this.addCatalog(tableIdentifier, Operation.CACHE)
      }
      case cacheTable: CacheTableAsSelect => {
        val tableIdentifier = this.toFireTableIdentifier(cacheTable.tableIdent)
        this.addCatalog(tableIdentifier, Operation.CACHE)
      }
      case uncacheTable: UncacheTable => {
        val tableIdentifier = this.toFireTableIdentifier(uncacheTable.tableIdent)
        this.addCatalog(tableIdentifier, Operation.UNCACHE)
      }*/
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
      // case p: InMemoryTableScanExec => handleInMemoryTableScan(p.relation.child)
      case p: InMemoryTableScanExec => handleInMemoryTableScan(p.relation.cachedPlan)
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
