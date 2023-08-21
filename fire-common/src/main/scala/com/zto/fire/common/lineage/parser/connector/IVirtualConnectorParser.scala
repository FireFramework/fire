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
import com.zto.fire.common.bean.lineage.SQLTable
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.{DatasourceDesc, SqlToDatasource}
import com.zto.fire.common.lineage.parser.ConnectorParser
import com.zto.fire.common.lineage.parser.ConnectorParser.toOperationSet

import java.util.Objects
import scala.collection.mutable

/**
 * 虚拟数据connector通用解析器
 *
 * @author ChengLong
 * @Date 2023/8/21 09:23
 * @version 2.3.
 */
trait IVirtualConnectorParser  extends ConnectorParser {

  /**
   * 解析指定的connector血缘
   *
   * @param tableIdentifier
   * 表的唯一标识
   * @param properties
   * connector中的options信息
   */
  override def parse(tableIdentifier: TableIdentifier, properties: mutable.Map[String, String], partitions: String): Unit = {
    val connector = properties.getOrElse("connector", "")
    if (noEmpty(connector)) {
      this.addDatasource(Datasource.parse(connector), Operation.CREATE_TABLE)
    }
  }

  /**
   * 添加一条虚拟数据源埋点信息
   *
   * @param datasource
   * 数据源类型
   */
  def addDatasource(datasource: Datasource, operation: Operation*): Unit = {
    if (this.canAdd) this.addDatasource(datasource, VirtualDatasource(datasource.toString, toOperationSet(operation: _*)))
  }
}

/**
 * 虚拟数据源
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param operation
 * 数据源操作类型（必须是var变量，否则合并不成功）
 */
case class VirtualDatasource(datasource: String, var operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[VirtualDatasource]
    Objects.equals(datasource, target.datasource)
  }

  override def hashCode(): Int = Objects.hash(datasource)
}

object VirtualDatasource extends SqlToDatasource {

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
    val operationSet = table.getOperationType

    if (this.isMatch("datagen", table)) {
      DatagenConnectorParser.addDatasource(Datasource.DATAGEN, operationSet.toSeq: _*)
    }

    if (this.isMatch("print", table)) {
      DatagenConnectorParser.addDatasource(Datasource.PRINT, operationSet.toSeq: _*)
    }
  }
}