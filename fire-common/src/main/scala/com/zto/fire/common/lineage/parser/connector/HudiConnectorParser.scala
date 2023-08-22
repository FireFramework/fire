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
import com.zto.fire.common.lineage.parser.ConnectorParser
import com.zto.fire.common.lineage.parser.ConnectorParser.toOperationSet
import com.zto.fire.common.lineage.{DatasourceDesc, SQLLineageManager, SqlToDatasource}
import com.zto.fire.predef._

import java.util.Objects

/**
 * Hudi Connector血缘解析器
 *
 * @author ChengLong 2023-08-09 10:12:19
 * @since 2.3.8
 */
private[fire] object HudiConnectorParser extends ConnectorParser {

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

    // 从建表语句的主键中获取
    var recordkey = SQLLineageManager.getTableInstance(tableIdentifier).getPrimaryKey.mkString(",")
    // 如果主键中未指定，则从with的选项中获取
    if (isEmpty(recordkey)) recordkey = properties.getOrElse("hoodie.datasource.write.recordkey.field", "")
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
      SQLLineageManager.getTableInstance(tableIdentifier).getPartitionField.mkString(","),
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
                    partitionField: String, operation: Operation*): Unit = {
    if (this.canAdd) this.addDatasource(datasource, HudiDatasource(datasource.toString, cluster, tableName, tableType, recordKey, precombineKey, partitionField, toOperationSet(operation: _*)))
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
                          var partitionField: String = null, var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {
  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[HudiDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(tableName, target.tableName)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster)

  /**
   * 单独的set方法可用于将target中的字段值set到对应的Datasource子类中
   * 注：若子类有些特殊字段需要被赋值，则需要覆盖此方法的实现
   *
   * @param target
   * 目标对象实例
   */
  override def set(target: DatasourceDesc): Unit = {
    if (target != null) {
      target match {
        case targetDesc: HudiDatasource =>
          if (noEmpty(targetDesc.tableType)) this.tableType = targetDesc.tableType
          if (noEmpty(targetDesc.recordKey)) this.recordKey = targetDesc.recordKey
          if (noEmpty(targetDesc.precombineKey)) this.precombineKey = targetDesc.precombineKey
          if (noEmpty(targetDesc.partitionField)) this.partitionField = targetDesc.partitionField
        case _ =>
      }
    }
  }
}

object HudiDatasource extends SqlToDatasource {

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
  def mapDatasource(table: SQLTable): Unit = {
    if (this.isNotMatch("hudi", table)) return

    HudiConnectorParser.addDatasource(Datasource.HUDI, HudiDatasource(Datasource.HUDI.toString, table.getCluster, table.getPhysicalTable, partitionField = table.getPartitionField.mkString(","), operation = table.getOperationType))
  }
}
