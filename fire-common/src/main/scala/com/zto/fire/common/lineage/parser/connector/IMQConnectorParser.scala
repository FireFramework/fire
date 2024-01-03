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
import com.zto.fire.common.bean.lineage.SQLTable
import com.zto.fire.common.conf.{FireKafkaConf, FireRocketMQConf}
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.{DatasourceDesc, SqlToDatasource}
import com.zto.fire.common.lineage.parser.ConnectorParser
import com.zto.fire.common.lineage.parser.ConnectorParser.toOperationSet

import java.util.Objects

/**
 * MQ类别通用父类
 *
 * @author ChengLong 2023-08-10 10:15:05
 * @since 2.3.8
 */
trait IMQConnectorParser extends ConnectorParser {

  /**
   * 添加一条MQ的埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群标识
   * @param topics
   * 主题列表
   * @param groupId
   * 消费组标识
   */
  private[fire] def addDatasource(datasource: Datasource, cluster: String, topics: String, groupId: String, operation: Operation*): Unit = {
    if (this.canAdd) {
      val url = if (Datasource.ROCKETMQ == datasource) FireRocketMQConf.rocketNameServer(cluster) else FireKafkaConf.kafkaBrokers(cluster)
      this.addDatasource(datasource, MQDatasource(datasource.toString, url, topics, groupId, toOperationSet(operation: _*)))
    }
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
    var groupId = ""

    // 判断数据源类型
    val datasource = if (isMatch("kafka", table) || table.getConnector.contains("kafka")) {
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
      KafkaConnectorParser.addDatasource(datasource, table.getCluster, table.getPhysicalTable, groupId, table.getOperationType.toSeq: _*)
    }
  }
}