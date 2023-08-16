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

package com.zto.fire.common.lineage.parser.connector

import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.bean.lineage.SQLTable
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.parser.ConnectorParser.toOperationSet
import com.zto.fire.common.lineage.{DatasourceDesc, LineageManager, SQLLineageManager, SqlToDatasource}
import com.zto.fire.predef._

import java.util.Objects
import scala.collection.mutable

/**
 * JDBC Connector血缘解析器
 *
 * @author ChengLong 2023-08-09 10:12:19
 * @since 2.3.8
 */
private[fire] object JDBCConnector extends IJDBCConnector {

  /**
   * 解析指定的connector血缘
   *
   * @param tableIdentifier
   * 表的唯一标识
   * @param properties
   * connector中的options信息
   */
  override def parse(tableIdentifier: TableIdentifier, properties: mutable.Map[String, String], partitions: String): Unit = {
    val tableName = properties.getOrElse("table-name", "")
    SQLLineageManager.setPhysicalTable(tableIdentifier, tableName)
    val url = properties.getOrElse("url", "")
    SQLLineageManager.setCluster(tableIdentifier, url)
    val username = properties.getOrElse("username", "")
    if (this.canAdd) LineageManager.addDBSql(Datasource.JDBC, url, username, "", toOperationSet(Operation.CREATE_TABLE, Operation.SELECT))
  }
}

/**
 * 面向数据库类型的数据源，带有tableName
 *
 * @param datasource
 *                  数据源类型，参考DataSource枚举
 * @param cluster
 *                  数据源的集群标识
 * @param tableName
 *                  表名
 * @param username
 *                  使用关系型数据库时作为jdbc的用户名，HBase留空
 * @param operation 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class DBDatasource(datasource: String, cluster: String,
                        tableName: String, username: String = "",
                        var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[DBDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(tableName, target.tableName) && Objects.equals(username, target.username)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster, tableName, username)
}

/**
 * 面向数据库类型的数据源，需将SQL中的tableName主动解析
 *
 * @param datasource
 *                  数据源类型，参考DataSource枚举
 * @param cluster
 *                  数据源的集群标识
 * @param username
 *                  使用关系型数据库时作为jdbc的用户名，HBase留空
 * @param sql       执行的SQL语句
 * @param operation 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class DBSqlSource(datasource: String, cluster: String, username: String,
                       sql: String, var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[DBSqlSource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(username, target.username) && Objects.equals(sql, target.sql)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster, username, sql)
}

object DBDatasource extends SqlToDatasource {

  /**
   * 解析SQL血缘中的表信息并映射为数据源信息
   * 注：1. 新增子类的名称必须来自Datasource枚举中map所定义的类型，如catalog为hudi，则Datasource枚举中映射为HudiDatasource，对应创建名为HudiDatasource的object继承该接口
   *    2. 新增Datasource子类需实现该方法，定义如何将SQLTable映射为对应的Datasource实例
   *
   * @param table
   * sql语句中使用到的表
   * @return
   * DatasourceDesc
   */
  override def mapDatasource(table: SQLTable): Unit = {
    if (table == null) return
    var datasource: Datasource = Datasource.UNKNOWN

    if (isMatch("jdbc", table)) {
      datasource = Datasource.JDBC
    }

    if (isMatch("clickhouse", table)) {
      datasource = Datasource.CLICKHOUSE
    }

    if (isMatch("doris", table)) {
      datasource = Datasource.DORIS
    }

    if (Datasource.UNKNOWN == datasource) return

    val options = table.getOptions
    val username = if (noEmpty(options)) options.getOrDefault("username", "") else ""
    val operations = new JHashSet[Operation]()
    table.getOperation.map(t => operations.add(Operation.parse(t)))
    JDBCConnector.addDatasource(datasource, table.getCluster, table.getPhysicalTable, username, operations.toSeq: _*)
  }
}