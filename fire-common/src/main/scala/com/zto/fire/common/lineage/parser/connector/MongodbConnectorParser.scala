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
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.SQLLineageManager
import com.zto.fire.common.util.RegularUtils

/**
 * Mongodb Connector血缘解析器
 *
 * @author ChengLong 2023-08-21 14:06:23
 * @since 2.3.8
 */
private[fire] object MongodbConnectorParser extends IJDBCConnectorParser {

  /**
   * 解析链接mongodb url中的username与password
   *
   * @param url
   * 连接串
   * @return
   * (username、password)
   */
  def parseMongoDBUrl(url: String): (String, String) = {
    val pattern = "mongodb://(.*?):(.*?)@"
    val regex = pattern.r
    val matcher = regex.findFirstMatchIn(url)

    matcher match {
      case Some(m) =>
        val user = m.group(1)
        val password = m.group(2)
        (user, password)
      case None =>
        ("", "")
    }
  }

  /**
   * 隐藏url中的敏感信息
   *
   * @param url
   * 链接url
   * @return
   * 脱敏后的url
   */
  def hideSensitive(url: String): String = {
    val (_, password) = this.parseMongoDBUrl(url)
    if (isEmpty(password)) url else url.replaceAll(password, RegularUtils.hidePassword)
  }

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
    val dbName = properties.getOrElse("database", "")
    val collection = properties.getOrElse("collection", "")
    val tableName = if (noEmpty(dbName)) s"$dbName.$collection" else collection
    SQLLineageManager.setPhysicalTable(tableIdentifier, tableName)

    val url = properties.getOrElse("uri", "")
    val (username, password) = this.parseMongoDBUrl(url)

    SQLLineageManager.setCluster(tableIdentifier, url)
    if (this.canAdd) this.addDatasource(Datasource.MONGODB, url, tableName, username, Operation.CREATE_TABLE)
  }
}
