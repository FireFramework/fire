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
import com.zto.fire.common.conf.FireKafkaConf
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.parser.ConnectorParser
import com.zto.fire.common.lineage.{DatasourceDesc, LineageManager, SQLLineageManager}
import com.zto.fire.predef.{JHashSet, JSet}

import java.util.Objects
import scala.collection.mutable

/**
 * Hive Connector血缘解析器
 *
 * @author ChengLong 2023-08-09 10:12:19
 * @since 2.3.8
 */
private[fire] object HiveConnector extends ConnectorParser {

  /**
   * 解析指定的connector血缘
   *
   * @param tableIdentifier
   * 表的唯一标识
   * @param properties
   * connector中的options信息
   */
  override def parse(tableIdentifier: TableIdentifier, properties: mutable.Map[String, String], partitions: String): Unit = {
  }

  /**
   * 添加一条Hive数据源埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群标识
   */
  private[fire] def addHiveDatasource(datasource: Datasource, cluster: String, tableName: String, partitions: JSet[SQLTablePartitions], operation: Operation*): Unit = {
    this.addDatasource(datasource, HiveDatasource(datasource.toString, cluster, tableName, partitions, toOperationSet(operation: _*)))
  }
}



/**
 * hive数据源
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param tableName
 * hive表名
 * @param operation
 * 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class HiveDatasource(datasource: String, cluster: String, tableName: String,
                          partitions: JSet[SQLTablePartitions],
                          var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[HiveDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(tableName, target.tableName)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster)
}
