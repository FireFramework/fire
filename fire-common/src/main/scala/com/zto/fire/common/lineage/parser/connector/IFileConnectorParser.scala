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

import com.zto.fire.common.bean.lineage.{SQLTable, SQLTablePartitions}
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.{DatasourceDesc, SqlToDatasource}
import com.zto.fire.common.lineage.parser.ConnectorParser
import com.zto.fire.common.lineage.parser.ConnectorParser.toOperationSet
import com.zto.fire.predef._

import java.util.Objects

/**
 * 文件类 Connector血缘解析器
 *
 * @author ChengLong 2023-08-21 13:21:35
 * @since 2.3.8
 */
trait IFileConnectorParser extends ConnectorParser {

  /**
   * 添加一条FileSystem数据源埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群标识
   */
  private[fire] def addDatasource(datasource: Datasource, cluster: String, tableName: String, partitions: JSet[SQLTablePartitions], operation: Operation*): Unit = {
    this.addDatasource(datasource, FileDatasource(datasource.toString, cluster, tableName, partitions, toOperationSet(operation: _*)))
  }
}

/**
 * Filesystem数据源
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param tableName
 * 文件系统表名
 * @param operation
 * 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class FileDatasource(datasource: String, cluster: String, tableName: String,
                          partitions: JSet[SQLTablePartitions],
                          var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[FileDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(tableName, target.tableName)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster)
}

object FileDatasource extends SqlToDatasource {

  /**
   * 解析SQL血缘中的表信息并映射为数据源信息
   * 注：1. 新增子类的名称必须来自Datasource枚举中map所定义的类型，如catalog为hudi，则Datasource枚举中映射为HudiDatasource，对应创建名为HudiDatasource的object继承该接口
   * 2. 新增Datasource子类需实现该方法，定义如何将SQLTable映射为对应的Datasource实例
   *
   * @param table
   * sql语句中使用到的表
   * @return
   * DatasourceDesc
   */
  def mapDatasource(table: SQLTable): Unit = {
    if (this.isMatch("filesystem", table)) {
      FilesystemConnectorParser.addDatasource(Datasource.FILESYSTEM, table.getCluster, table.getPhysicalTable, table.getPartitions, table.getOperationType.toSeq: _*)
    }

    if (this.isMatch("iceberg", table)) {
      IcebergConnectorParser.addDatasource(Datasource.ICEBERG, table.getCluster, table.getPhysicalTable, table.getPartitions, table.getOperationType.toSeq: _*)
    }
  }
}
