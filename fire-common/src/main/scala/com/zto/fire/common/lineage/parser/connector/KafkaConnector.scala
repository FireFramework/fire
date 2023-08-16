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
import com.zto.fire.common.conf.FireKafkaConf
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.{DatasourceDesc, SQLLineageManager, SqlToDatasource}
import com.zto.fire.predef._

import java.util.Objects
import scala.collection.mutable

/**
 * Kafka Connector血缘解析器
 *
 * @author ChengLong 2023-08-09 10:12:19
 * @since 2.3.8
 */
private[fire] object KafkaConnector extends IMQConnector {

  /**
   * 解析指定的connector血缘
   *
   * @param tableIdentifier
   * 表的唯一标识
   * @param properties
   * connector中的options信息
   */
  override def parse(tableIdentifier: TableIdentifier, properties: mutable.Map[String, String], partitions: String): Unit = {
    val url = properties.getOrElse("properties.bootstrap.servers", "")
    SQLLineageManager.setCluster(tableIdentifier, FireKafkaConf.kafkaBrokers(url))
    val topic = properties.getOrElse("topic", "")
    SQLLineageManager.setPhysicalTable(tableIdentifier, topic)
    val groupId = properties.getOrElse("properties.group.id", "")
    this.addDatasource(Datasource.KAFKA, url, topic, groupId, Operation.CREATE_TABLE)
  }
}

/**
 * MQ类型数据源，如：kafka、RocketMQ等
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param topics
 * 使用到的topic列表
 * @param groupId
 * 任务的groupId
 * @param operation
 * 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class MQDatasource(datasource: String, cluster: String, topics: String,
                        groupId: String, var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[MQDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(topics, target.topics)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster, topics, groupId)
}

object MQDatasource extends SqlToDatasource {

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
    val operations = new JHashSet[Operation]()
    table.getOperation.map(t => operations.add(Operation.parse(t)))
    var groupId = ""

    // 判断数据源类型
    val datasource = if (isMatch("kafka", table)) {
      if (noEmpty(table.getOptions)) {
        groupId = table.getOptions.getOrDefault("properties.group.id", "")
      }

      Datasource.KAFKA
    } else if (isMatch("fire-rocketmq", table) || isMatch("rocketmq", table)) {
      if (noEmpty(table.getOptions)) {
        groupId = table.getOptions.getOrDefault("rocket.group.id", "")
        if (isEmpty(groupId)) {
          // 兼容rocketmq-flink社区connector
          groupId = table.getOptions.getOrDefault("consumerGroup", "")
        }
      }

      Datasource.ROCKETMQ
    } else {
      Datasource.UNKNOWN
    }

    if (datasource != Datasource.UNKNOWN) {
      KafkaConnector.addDatasource(datasource, table.getCluster, table.getPhysicalTable, groupId, operations.toSeq: _*)
    }
  }
}