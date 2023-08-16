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

package com.zto.fire.common.lineage

import com.zto.fire.predef._
import com.zto.fire.common.bean.lineage.SQLTable

/**
 * 约定SQL中的Table对象转为Datasource数据源的接口实现
 *
 * @author ChengLong 2023-08-15 09:21:51
 * @since 2.3.8
 */
trait SqlToDatasource {

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
  def mapDatasource(table: SQLTable): Unit

  /**
   * 判断给定的connector与SQLTable是否匹配
   *
   * @param connector
   * hudi、kafka、es等
   * @param table
   * SQLTable实例
   * @return
   */
  protected def isMatch(connector: String, table: SQLTable): Boolean = {
    if (isEmpty(connector, table)) return false
    if (noEmpty(table.getConnector) && connector.equalsIgnoreCase(table.getConnector)) return true
    if (noEmpty(table.getCatalog) && connector.equalsIgnoreCase(table.getCatalog)) return true

    false
  }

  /**
   * 判断给定的connector与SQLTable是否匹配
   *
   * @param connector
   * hudi、kafka、es等
   * @param table
   * SQLTable实例
   * @return
   */
  protected def isNotMatch(connector: String, table: SQLTable): Boolean = !this.isMatch(connector, table)
}
