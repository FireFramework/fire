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
 * 当前fire未支持的connector血缘解析器
 * 注：由于第三方或开发者可能自定义connector，fire只解析该connector最基本的信息
 *
 * @author ChengLong 2023-08-25 15:14:36
 * @since 2.3.8
 */
private[fire] object UrlConnectorParser extends ConnectorParser {

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
    val connector = properties.getOrElse("connector", "")
    val finalConnector = if (noEmpty(connector)) connector else Datasource.URL.toString
    val url = properties.getOrElse("url", properties.getOrElse("path", properties.getOrElse("cluster", "")))
    SQLLineageManager.setCluster(tableIdentifier, url)
    this.addDatasource(finalConnector, url, Operation.CREATE_TABLE)
  }

  /**
   * 添加一条接口数据源埋点信息
   *
   * @param url
   * 数据源地址
   */
  def addDatasource(datasource: String, url: String, operation: Operation *): Unit = {
    if (this.canAdd) this.addDatasource(Datasource.valueOf(datasource), UrlDatasource(datasource, url, toOperationSet(operation: _*)))
  }
}

/**
 * InterfaceDatasource数据源
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param operation
 * 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class UrlDatasource(datasource: String, url: String, var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {
  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[UrlDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(url, target.url)
  }

  override def hashCode(): Int = Objects.hash(datasource)
}

object UrlDatasource extends SqlToDatasource {

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
    if (table == null) return
    var datasource: Datasource = Datasource.UNKNOWN

    if (isMatch("http", table)) {
      datasource = Datasource.HTTP
    }

    if (isMatch("rpc", table)) {
      datasource = Datasource.RPC
    }

    if (isMatch("dubbo", table)) {
      datasource = Datasource.DUBBO
    }

    if (isMatch("interface", table)) {
      datasource = Datasource.INTERFACE
    }

    if (isMatch("url", table)) {
      datasource = Datasource.URL
    }

    if (Datasource.UNKNOWN == datasource) return

    UrlConnectorParser.addDatasource(datasource.toString, table.getCluster, table.getOperationType.toSeq: _*)
  }
}
