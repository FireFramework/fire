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
 * Paimon Connector血缘解析器
 *
 * @author ChengLong 2023-08-22 16:01:42
 * @since 2.3.8
 */
private[fire] object PaimonConnectorParser extends ConnectorParser {

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
    val url = properties.getOrElse("warehouse", "")
    SQLLineageManager.setCluster(tableIdentifier, "")
    SQLLineageManager.setPhysicalTable(tableIdentifier, tableIdentifier.identifier)

    // 从建表语句的主键中获取
    val primaryKey = SQLLineageManager.getTableInstance(tableIdentifier).getPrimaryKey.mkString(",")
    val bucketKey = properties.getOrElse("bucket-key", "")

    this.addDatasource(Datasource.PAIMON, url,
      tableIdentifier.identifier,
      primaryKey,
      bucketKey,
      SQLLineageManager.getTableInstance(tableIdentifier).getPartitionField.mkString(","),
      Operation.CREATE_TABLE
    )
  }

  /**
   * 添加一条Paimon数据源埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群标识
   */
  def addDatasource(datasource: Datasource, cluster: String, tableName: String,
                    primaryKey: String, bucketKey: String,
                    partitionField: String, operation: Operation*): Unit = {
    if (this.canAdd) this.addDatasource(datasource, PaimonDatasource(datasource.toString, cluster, tableName, primaryKey, bucketKey, partitionField, toOperationSet(operation: _*)))
  }
}

/**
 * Paimon数据源
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param tableName
 * Paimon表名
 * @param operation
 * 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class PaimonDatasource(datasource: String, cluster: String,
                            tableName: String, var primaryKey: String = null,
                            var bucketKey: String = null,
                            var partitionField: String = null, var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {
  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[PaimonDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(tableName, target.tableName)
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
        case targetDesc: PaimonDatasource =>
          if (noEmpty(targetDesc.primaryKey)) this.primaryKey = targetDesc.primaryKey
          if (noEmpty(targetDesc.bucketKey)) this.bucketKey = targetDesc.bucketKey
          if (noEmpty(targetDesc.partitionField)) this.partitionField = targetDesc.partitionField
        case _ =>
      }
    }
  }
}

object PaimonDatasource extends SqlToDatasource {

  /**
   * 解析SQL血缘中的表信息并映射为数据源信息
   * 注：1. 新增子类的名称必须来自Datasource枚举中map所定义的类型，如catalog为hudi，则Datasource枚举中映射为HudiDatasource，对应创建名为HudiDatasource的object继承该接口
   *  2. 新增Datasource子类需实现该方法，定义如何将SQLTable映射为对应的Datasource实例
   *
   * @param table
   * sql语句中使用到的表
   * @return
   * DatasourceDesc
   */
  def mapDatasource(table: SQLTable): Unit = {
    if (this.isNotMatch("paimon", table)) return

    PaimonConnectorParser.addDatasource(Datasource.PAIMON, table.getCluster, table.getPhysicalTable, table.getPrimaryKey.mkString(","), "", table.getPartitionField.mkString(","), table.getOperationType.toSeq :_*)
  }
}
