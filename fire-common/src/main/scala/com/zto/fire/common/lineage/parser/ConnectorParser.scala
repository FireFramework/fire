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

package com.zto.fire.common.lineage.parser

import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.{DatasourceDesc, LineageManager}
import com.zto.fire.common.util.Logging
import com.zto.fire.predef._

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/**
 * 通用的connector解析
 *
 * @author ChengLong 2023-08-09 10:03:42
 * @since 2.3.8
 */
trait ConnectorParser extends Logging {
  // 用于统计添加到数据源列表中的记录数
  private lazy val count = new AtomicInteger()

  /**
   * 累加添加到血缘列表中的记录总数
   *
   * @return
   * true：可继续添加 false：已超额，不再添加
   */
  protected def canAdd: Boolean = {
    this.count.incrementAndGet() <= FireFrameworkConf.lineageMaxSize
  }

  /**
   * 解析指定的connector血缘
   *
   * @param tableIdentifier
   * 表的唯一标识
   * @param properties
   * connector中的options信息
   */
  def parse(tableIdentifier: TableIdentifier, properties: mutable.Map[String, String], partitions: String): Unit

  /**
   * 添加数据源信息
   *
   * @param sourceType
   * 数据源类型
   * @param datasourceDesc
   * 数据源描述
   */
  def addDatasource(sourceType: Datasource, datasourceDesc: DatasourceDesc): Unit = {
    if (FireFrameworkConf.lineageEnable) LineageManager.addDatasource(sourceType, datasourceDesc)
  }
}

object ConnectorParser {

  /**
   * 添加多个数据源操作
   */
  def toOperationSet(operation: Operation*): JHashSet[Operation] = {
    val operationSet = new JHashSet[Operation]
    operation.foreach(operationSet.add)
    operationSet
  }
}
