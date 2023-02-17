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

package com.zto.fire.flink.util

import com.zto.fire.common.util.ReflectionUtils
import org.apache.flink.table.api.TableResult

/**
 * Flink Table工具类
 *
 * @author ChengLong 2023-02-14 09:54:42
 * @since 2.3.2
 */
object TableUtils {
  val tableResultClass = "org.apache.flink.table.api.internal.TableResultImpl"
  val resultOKField = "TABLE_RESULT_OK"

  /**
   * 返回标志为成功的查询结果集
   *
   * 注：此处反射调用是为了兼容不同的Flink版本
   */
  val TABLE_RESULT_OK = {
    val tableResultClazz = Class.forName(this.tableResultClass)
    val field = ReflectionUtils.getFieldByName(tableResultClazz, this.resultOKField)
    field.setAccessible(true)
    field.get(null).asInstanceOf[TableResult]
  }

}
