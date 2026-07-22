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

import com.zto.fire.common.bean.lineage.LineageCollectData
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.common.util.Logging
import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.ConcurrentHashMap

/**
 * Fire框架实时血缘累加器，用于采集实时任务用到的数据源信息、SQL血缘信息、Fire API 使用血缘等
 * 支持：SQL、JDBC、Kafka、RocketMQ、HBase 等组件的血缘信息解析与采集，以及 API 用量分布式汇总
 *
 * @author ChengLong 2022-08-29 09:21:48
 * @since 2.3.2
 */
private[fire] class LineageAccumulator extends AccumulatorV2[LineageCollectData, LineageCollectData] with Logging {
  // 用于存放数据源 + API 血缘的采集载荷
  private val collectData = new LineageCollectData()

  /**
   * 判断累加器是否为空
   */
  override def isZero: Boolean = this.collectData.isEmpty

  /**
   * 用于复制累加器
   */
  override def copy(): AccumulatorV2[LineageCollectData, LineageCollectData] = {
    val acc = new LineageAccumulator
    LineageManager.mergeLineageCollectData(acc.value, this.collectData)
    acc
  }

  /**
   * driver端执行有效，用于清空累加器
   */
  override def reset(): Unit = {
    this.collectData.setDatasource(new ConcurrentHashMap())
    this.collectData.setApis(new java.util.ArrayList())
  }

  /**
   * 将新的血缘信息添加到累加器中
   */
  override def add(v: LineageCollectData): Unit = {
    if (FireFrameworkConf.accEnable && v != null && !v.isEmpty) {
      LineageManager.mergeLineageCollectData(this.collectData, v)
    }
  }

  /**
   * executor端向driver端merge累加数据
   *
   * @param other
   * executor端累加结果
   */
  override def merge(other: AccumulatorV2[LineageCollectData, LineageCollectData]): Unit = {
    if (other != null && other.value != null && !other.value.isEmpty) {
      LineageManager.mergeLineageCollectData(this.collectData, other.value)
    }
  }

  /**
   * driver端获取累加器的值
   *
   * @return
   * 收集到的血缘信息
   */
  override def value: LineageCollectData = this.collectData
}
