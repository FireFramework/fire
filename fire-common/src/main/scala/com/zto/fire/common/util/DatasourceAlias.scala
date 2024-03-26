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

package com.zto.fire.common.util

import com.zto.fire.common.lineage.parser.connector.{DBDatasource, FileDatasource, MQDatasource, VirtualDatasource}

/**
 * 数据源别名映射
 *
 * @author ChengLong
 * @Date 2024/3/15 09:17
 * @version 2.4.4
 */
trait DatasourceAlias {
  // 虚拟数据源
  type DataGenDatasource = VirtualDatasource
  type PrintDatasource = VirtualDatasource
  type BlackholeDatasource = VirtualDatasource

  // 消息队列数据源
  type KafkaDatasource = MQDatasource
  type UpsertKafkaDatasource = MQDatasource
  type RocketMQDatasource = MQDatasource
  type FireRocketMQDatasource = MQDatasource

  // 文件数据源
  type IcebergDatasource = FileDatasource
  type DynamodbDatasource = FileDatasource
  type FirehoseDatasource = FileDatasource
  type KinesisDatasource = FileDatasource

  // 数据库数据源
  type JdbcDatasource = DBDatasource
  type PostgreSQLDatasource = DBDatasource
  type MySQLDatasource = DBDatasource
  type TiDBLDatasource = DBDatasource
  type OracleDatasource = DBDatasource
  type SQLServerDatasource = DBDatasource
  type DB2Datasource = DBDatasource
  type ClickhouseDatasource = DBDatasource
  type PrestoDatasource = DBDatasource
  type KylinDatasource = DBDatasource
  type DerbyDatasource = DBDatasource
  type HBaseDatasource = DBDatasource
  type RedisDatasource = DBDatasource
  type ESDatasource = DBDatasource
  type MongodbDatasource = DBDatasource
  type DorisDatasource = DBDatasource
  type StarRocksDatasource = DBDatasource
  type InfluxdbDatasource = DBDatasource
  type PromethusDatasource = DBDatasource
}
