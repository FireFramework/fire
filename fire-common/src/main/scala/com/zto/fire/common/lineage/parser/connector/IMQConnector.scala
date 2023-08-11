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
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.DatasourceDesc
import com.zto.fire.common.lineage.parser.ConnectorParser
import com.zto.fire.common.util.ReflectionUtils

import scala.reflect.ClassTag

/**
 * MQ类别通用父类
 *
 * @author ChengLong 2023-08-10 10:15:05
 * @since 2.3.8
 */
trait IMQConnector extends ConnectorParser {

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
    this.addDatasource(datasource, MQDatasource(datasource.toString, cluster, topics, groupId, toOperationSet(operation: _*)))
  }

}
