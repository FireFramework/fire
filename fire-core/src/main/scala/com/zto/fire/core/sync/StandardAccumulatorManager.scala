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

package com.zto.fire.core.sync

import com.zto.fire.common.bean.standard.StandardResult
import com.zto.fire.predef._

import java.util

/**
 * 用于将各个 container 端代码标准化分析结果收集到 master 端，按 {@link StandardResult#equals} 去重。
 *
 * @author ChengLong
 * @since 3.0.0
 */
trait StandardAccumulatorManager extends SyncManager {
  private lazy val standardResults = java.util.Collections.synchronizedSet(new JLinkedHashSet[StandardResult]())

  /**
   * 添加单条代码标准化分析结果
   */
  def add(result: StandardResult): Unit = {
    if (result != null) this.standardResults.add(result)
  }

  /**
   * 批量添加代码标准化分析结果
   */
  def add(results: util.Collection[StandardResult]): Unit = {
    if (results != null && !results.isEmpty) {
      val iterator = results.iterator()
      while (iterator.hasNext) {
        this.add(iterator.next())
      }
    }
  }

  /**
   * 获取当前已收集到的代码标准化分析结果
   */
  def getValue: JSet[StandardResult] = new JLinkedHashSet[StandardResult](this.standardResults)

  /**
   * 获取并清空当前已收集到的代码标准化分析结果，避免定时发送重复消息
   */
  def getAndReset: JSet[StandardResult] = this.synchronized {
    val results = new JLinkedHashSet[StandardResult](this.standardResults)
    this.standardResults.clear()
    results
  }

  /**
   * 清空已收集到的代码标准化分析结果
   */
  def reset(): Unit = this.standardResults.clear()
}
