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

import com.zto.fire.predef._
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.SQLLineageManager

import scala.collection.mutable

/**
 * JDBC Connector血缘解析器
 *
 * @author ChengLong 2023-08-09 10:12:19
 * @since 2.3.8
 */
private[fire] object CdcConnectorParser extends IJDBCConnectorParser {

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
    val dbName = properties.getOrElse("database-name", "")
    val physicalTable = if (noEmpty(dbName)) s"$dbName.$tableName" else tableName
    SQLLineageManager.setPhysicalTable(tableIdentifier, physicalTable)

    val url = properties.getOrElse("hostname", properties.getOrElse("hosts", ""))
    val port = properties.getOrElse("port", "")
    val cluster = if (noEmpty(port)) s"$url:$port" else url
    SQLLineageManager.setCluster(tableIdentifier, cluster)

    // 获取数据源类型
    val connector = properties.getOrElse("connector", "").toLowerCase.replaceAll("-cdc", "")
    val datasource = Datasource.parse(connector)
    val finalDatasource = if (Datasource.UNKNOWN == datasource) Datasource.JDBC else datasource

    val username = properties.getOrElse("username", "")

    if (this.canAdd) this.addDatasource(finalDatasource, cluster, physicalTable, username, Operation.CREATE_TABLE)
  }

}