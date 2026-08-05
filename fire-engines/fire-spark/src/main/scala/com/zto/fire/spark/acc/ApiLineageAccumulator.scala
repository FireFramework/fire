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

package com.zto.fire.spark.acc

import com.zto.fire._
import com.zto.fire.common.bean.lineage.ApiLineage
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.common.util.Logging
import org.apache.spark.util.AccumulatorV2

import java.util
import java.util.concurrent.ConcurrentHashMap

/**
 * Fire API 使用血缘累加器：将 executor 端采集的 API 用量汇总到 driver。
 * 与 datasource 血缘累加器相互独立，对外仍由 {@code Lineage.apis} 与 datasource 并列输出。
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] class ApiLineageAccumulator
  extends AccumulatorV2[util.List[ApiLineage], ConcurrentHashMap[String, ApiLineage]] with Logging {

  private val apiMap = new ConcurrentHashMap[String, ApiLineage]()

  override def isZero: Boolean = this.apiMap.isEmpty

  override def copy(): AccumulatorV2[util.List[ApiLineage], ConcurrentHashMap[String, ApiLineage]] = {
    val acc = new ApiLineageAccumulator
    LineageManager.mergeApiLineage(acc.value, this.apiMap.values())
    acc
  }

  override def reset(): Unit = this.apiMap.clear()

  override def add(v: util.List[ApiLineage]): Unit = {
    if (FireFrameworkConf.accEnable && v != null && !v.isEmpty) {
      LineageManager.mergeApiLineage(this.apiMap, v)
    }
  }

  override def merge(other: AccumulatorV2[util.List[ApiLineage], ConcurrentHashMap[String, ApiLineage]]): Unit = {
    if (other != null && other.value != null && !other.value.isEmpty) {
      LineageManager.mergeApiLineage(this.apiMap, other.value.values())
    }
  }

  override def value: ConcurrentHashMap[String, ApiLineage] = this.apiMap
}
