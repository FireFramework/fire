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

/**
 * 数据源描述
 *
 * @author ChengLong 2023-08-09 16:51:32
 * @since 2.0.0
 */
trait DatasourceDesc {

  /**
   * 单独的set方法可用于将target中的字段值set到对应的Datasource子类中
   * 注：若子类有些特殊字段需要被赋值，则需要覆盖此方法的实现
   *
   * @param target
   * 目标对象实例
   */
  def set(target: DatasourceDesc): Unit = {}
}