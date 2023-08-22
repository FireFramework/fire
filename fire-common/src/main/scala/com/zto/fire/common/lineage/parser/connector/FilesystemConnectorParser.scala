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
import com.zto.fire.common.bean.lineage.SQLTablePartitions
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.SQLLineageManager

import java.util.Collections

/**
 * FileSystem Connector血缘解析器
 *
 * @author ChengLong 2023-08-21 13:21:35
 * @since 2.3.8
 */
private[fire] object FilesystemConnectorParser extends IFileConnectorParser {

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
    val path = properties.getOrElse("path", "")
    SQLLineageManager.setCluster(tableIdentifier, path)
    SQLLineageManager.setPhysicalTable(tableIdentifier, tableIdentifier.identifier)

    this.addDatasource(Datasource.FILESYSTEM, path,
      tableIdentifier.identifier,
      Collections.emptySet[SQLTablePartitions](),
      Operation.CREATE_TABLE
    )
  }
}