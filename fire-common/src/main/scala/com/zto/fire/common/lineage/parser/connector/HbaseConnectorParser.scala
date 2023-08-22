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
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.SQLLineageManager

/**
 * HBase Connector血缘解析器
 *
 * @author ChengLong 2023-08-10 10:41:13
 * @since 2.3.8
 */
private[fire] object HbaseConnectorParser extends IJDBCConnectorParser {

  /**
   * 解析指定的connector血缘
   * 注：可获取到主键、分区字段等信息：SQLLineageManager.getTableInstance(tableIdentifier).getPrimaryKey
   *
   * @param tableIdentifier
   * 表的唯一标识
   * @param properties
   * connector中的options信息
   */
  override def parse(tableIdentifier: TableIdentifier, properties: Map[String, String]): Unit = {
    val url = properties.getOrElse("zookeeper.quorum", "")
    SQLLineageManager.setCluster(tableIdentifier, url)
    val physicalTable = properties.getOrElse("table-name", "")
    SQLLineageManager.setPhysicalTable(tableIdentifier, physicalTable)
    this.addDatasource(Datasource.HBASE, url, physicalTable, "", Operation.CREATE_TABLE)
  }
}