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

import com.zto.fire.common.bean.lineage.{SQLTableColumns, _}
import com.zto.fire.predef._

/**
 * SQL血缘解析管理器，协助快速构建SQL血缘信息
 *
 * @author ChengLong 2022-09-01 15:10:38
 * @since 2.2.3
 */
private[fire] object SQLLineageManager {
  private lazy val sqlLineage = new SQLLineage()
  private lazy val relationSet = new JHashSet[SQLTableRelations]()
  private lazy val tableLineageMap = new JConcurrentHashMap[String, SQLTable]()

  /**
   * 维护表与表之间的关系
   *
   * @param srcTable
   * 数据来源表
   * @param sinkTable
   * 目标表
   */
  def addRelation(srcTable: String, sinkTable: String): Unit = {
    this.relationSet.add(new SQLTableRelations(srcTable, sinkTable))
  }

  /**
   * 获取SQL血缘信息
   */
  def getSQLLineage: SQLLineage = {
    this.sqlLineage.setTables(this.tableLineageMap.values().toList)
    this.sqlLineage.setRelations(this.relationSet.toList)
    this.sqlLineage
  }

  /**
   * 根据给定的库表名称获取完整表名
   *
   * @param dbName
   * 数据库名称（可为空）
   * @param tableName
   * 表名
   * @return
   * dbName.tableName
   */
  def getTableIdentify(dbName: String, tableName: String): String = {
    requireNonEmpty(tableName, "表名不能为空")
    val finalDBName = if (isEmpty(dbName)) "" else dbName
    s"${finalDBName}.${tableName}"
  }

  /**
   * 根据库表信息获取SQLTable实例
   *
   * @param dbName
   * 数据库名称，可为空
   * @param tableName
   * 表名
   * @return
   * SQLTable
   */
  def getTableInstance(dbName: String, tableName: String): SQLTable = {
    this.tableLineageMap.mergeGet(getTableIdentify(dbName, tableName)) {
      new SQLTable()
    }
  }

  /**
   * 用于为指定的SQLTable对象添加必要的字段值
   */
  private[this] def setTableField(dbName: String, tableName: String)(fun: SQLTable => Unit): SQLTable = {
    val table = this.getTableInstance(dbName, tableName)
    fun(table)
    table
  }

  /**
   * 为指定的表添加options信息
   *
   * @param options
   * 选项信息
   */
  def setOptions(dbName: String, tableName: String, options: Map[String, String]): SQLTable = {
    this.setTableField(dbName, tableName) {
      _.getOptions.putAll(options)
    }
  }

  /**
   * 为指定的表添加操作信息
   *
   * @param operations
   * 操作类型信息（INSERT、DROP等）
   */
  def setOperation(dbName: String, tableName: String, operations: String*): SQLTable = {
    this.setTableField(dbName, tableName) {
      _.getOperation.addAll(operations)
    }
  }

  /**
   * 为指定的表添加使用到的字段信息
   *
   * @param columns
   * 字段列表
   */
  def setColumns(dbName: String, tableName: String, columns: SQLTableColumns*): SQLTable = {
    this.setTableField(dbName, tableName) {
      _.getColumns.addAll(columns)
    }
  }

  /**
   * 为指定的表添加catalog信息
   *
   * @param catalog
   * catalog信息：hive、kafka、jdbc等
   */
  def setCatalog(dbName: String, tableName: String, catalog: String): SQLTable = {
    this.setTableField(dbName, tableName) {
      _.setCatalog(catalog)
    }
  }

  /**
   * 为指定的表添加catalog的集群url
   *
   * @param cluster
   * 集群地址
   */
  def setCluster(dbName: String, tableName: String, cluster: String): SQLTable = {
    this.setTableField(dbName, tableName) {
      _.setCluster(cluster)
    }
  }

  /**
   * 为指定的表添加catalog的具体物理表名
   *
   * @param physicalTable
   * 真实的表名
   */
  def setPhysicalTable(dbName: String, tableName: String, physicalTable: String): SQLTable = {
    this.setTableField(dbName, tableName) {
      _.setPhysicalTable(physicalTable)
    }
  }

  /**
   * 为指定的表添加视图名称
   *
   * @param tmpView
   * spark或flink任务内部注册的临时表名
   */
  def setTmpView(dbName: String, tableName: String, tmpView: String): SQLTable = {
    this.setTableField(dbName, tableName) {
      _.setTmpView(tmpView)
    }
  }
}
