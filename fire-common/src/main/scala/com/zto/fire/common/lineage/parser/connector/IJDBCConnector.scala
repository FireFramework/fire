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

import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.parser.ConnectorParser
import com.zto.fire.common.lineage.parser.ConnectorParser.toOperationSet

/**
 * JDBC类别通用父类
 *
 * @author ChengLong 2023-08-10 10:15:05
 * @since 2.3.8
 */
trait IJDBCConnector extends ConnectorParser {

  /**
   * 添加一条DB的埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群信息
   * @param tableName
   * 表名
   * @param username
   * 连接用户名
   */
  def addDatasource(datasource: Datasource, cluster: String, tableName: String, username: String, operation: Operation*): Unit = {
    if (this.canAdd) this.addDatasource(datasource, DBDatasource(datasource.toString, cluster, tableName, username, toOperationSet(operation: _*)))
  }
}