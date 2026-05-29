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

package com.zto.fire.spark.sync

import com.zto.fire.common.bean.standard.Standard
import com.zto.fire.core.sync.StandardAccumulatorManager
import com.zto.fire.predef._
import com.zto.fire.spark.acc.AccumulatorManager

import java.util._

/**
 * 用于将各个Executor端代码标准化检测结果收集到Driver端
 *
 * @author ChengLong
 * @since 3.0.0
 */
object SparkStandardAccumulatorManager extends StandardAccumulatorManager {

  /**
   * 将代码标准化检测结果放到累加器中
   */
  override def add(standard: Standard): Unit = {
    AccumulatorManager.addStandard(standard)
  }

  /**
   * 批量添加代码标准化检测结果
   */
  override def add(standards: Collection[Standard]): Unit = {
    if (standards != null && !standards.isEmpty) {
      val iterator = standards.iterator()
      while (iterator.hasNext) {
        this.add(iterator.next())
      }
    }
  }

  /**
   * 获取收集到的代码标准化检测结果
   */
  override def getValue: JList[Standard] = new JArrayList[Standard](AccumulatorManager.getStandard)

  /**
   * 获取并清空收集到的代码标准化检测结果
   */
  override def getAndReset: JList[Standard] = AccumulatorManager.getAndResetStandard

  /**
   * 清空收集到的代码标准化检测结果
   */
  override def reset(): Unit = AccumulatorManager.standardAccumulator.reset()
}
