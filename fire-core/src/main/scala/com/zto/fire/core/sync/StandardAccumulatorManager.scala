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

import com.zto.fire.common.bean.standard.Standard
import com.zto.fire.predef._

import java.util
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * 用于将各个container端代码标准化分析结果收集到master端
 *
 * @author ChengLong
 * @since 3.0.0
 */
trait StandardAccumulatorManager extends SyncManager {
  private lazy val standardQueue = new ConcurrentLinkedQueue[Standard]()

  /**
   * 添加单条代码标准化分析结果
   */
  def add(standard: Standard): Unit = {
    if (standard != null) this.standardQueue.add(standard)
  }

  /**
   * 批量添加代码标准化分析结果
   */
  def add(standards: util.Collection[Standard]): Unit = {
    if (standards != null && !standards.isEmpty) {
      val iterator = standards.iterator()
      while (iterator.hasNext) {
        this.add(iterator.next())
      }
    }
  }

  /**
   * 获取当前已收集到的代码标准化分析结果
   */
  def getValue: JList[Standard] = new JArrayList[Standard](this.standardQueue)

  /**
   * 获取并清空当前已收集到的代码标准化分析结果，避免定时发送重复消息
   */
  def getAndReset: JList[Standard] = this.synchronized {
    val standards = new JArrayList[Standard]()
    var standard = this.standardQueue.poll()
    while (standard != null) {
      standards.add(standard)
      standard = this.standardQueue.poll()
    }
    standards
  }

  /**
   * 清空已收集到的代码标准化分析结果
   */
  def reset(): Unit = this.standardQueue.clear()
}
