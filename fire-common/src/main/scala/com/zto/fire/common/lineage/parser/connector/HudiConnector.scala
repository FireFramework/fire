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
import com.zto.fire.common.lineage.parser.ConnectorParser
import com.zto.fire.common.lineage.{DatasourceDesc, SQLLineageManager}
import com.zto.fire.predef._

import java.util.Objects
import scala.collection.mutable

/**
 * Hudi Connector血缘解析器
 *
 * @author ChengLong 2023-08-09 10:12:19
 * @since 2.3.8
 */
private[fire] object HudiConnector extends ConnectorParser {

  /**
   * 解析指定的connector血缘
   *
   * @param tableIdentifier
   * 表的唯一标识
   * @param properties
   * connector中的options信息
   */
  override def parse(tableIdentifier: TableIdentifier, properties: mutable.Map[String, String], partitions: String): Unit = {
    val path = properties.getOrElse("path", "")
    SQLLineageManager.setCluster(tableIdentifier, path)

    var recordkey = properties.getOrElse("hoodie.datasource.write.recordkey.field", "")
    if (isEmpty(recordkey)) recordkey = properties.getOrElse("hoodie.datasource.write.recordkey.field", "")

    var precombineField = properties.getOrElse("write.precombine.field", "")
    if (isEmpty(precombineField)) precombineField = properties.getOrElse("precombine.field", "")

    val hudiTable = this.getTableNameByPath(path)
    SQLLineageManager.setPhysicalTable(tableIdentifier, hudiTable)

    this.addDatasource(Datasource.HUDI, path,
      hudiTable,
      properties("table.type"),
      recordkey,
      precombineField,
      partitions,
      Operation.CREATE_TABLE
    )
  }

  /**
   * 根据hudi表存储路径获取表名
   *
   * @param path
   * hudi表存储url
   * @return
   * hudi表名
   */
  private[this] def getTableNameByPath(path: String): String = {
    if (isEmpty(path)) return ""
    val elements = path.split("/")
    elements(elements.length - 1)
  }

  /**
   * 添加一条Hudi数据源埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群标识
   */
  def addDatasource(datasource: Datasource, cluster: String, tableName: String,
                    tableType: String, recordKey: String, precombineKey: String,
                    partition: String, operation: Operation*): Unit = {
    this.addDatasource(datasource, HudiDatasource(datasource.toString, cluster, tableName, tableType, recordKey, precombineKey, partition, toOperationSet(operation: _*)))
  }
}

/**
 * Hudi数据源
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param tableName
 * hudi表名
 * @param operation
 * 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class HudiDatasource(datasource: String, cluster: String,
                          tableName: String, var tableType: String = null, var recordKey: String = null, var precombineKey: String = null,
                          var partition: String = null, var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {
  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[HudiDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(tableName, target.tableName)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster)

  def set(target: HudiDatasource): Unit = {
    if (target != null) {
      if (noEmpty(target.tableType)) this.tableType = target.tableType
      if (noEmpty(target.recordKey)) this.recordKey = target.recordKey
      if (noEmpty(target.precombineKey)) this.precombineKey = target.precombineKey
      if (noEmpty(target.partition)) this.partition = target.partition
    }
  }
}
