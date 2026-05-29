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

import com.zto.fire.common.bean.standard.Standard
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.Logging
import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Fire框架代码标准化检测累加器，用于将Executor端采集到的不规范API调用汇总到Driver端
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] class StandardAccumulator extends AccumulatorV2[Standard, ConcurrentLinkedQueue[Standard]] with Logging {
  private val standardQueue = new ConcurrentLinkedQueue[Standard]()

  /**
   * 判断累加器是否为空
   */
  override def isZero: Boolean = this.standardQueue.isEmpty

  /**
   * 用于复制累加器
   */
  override def copy(): AccumulatorV2[Standard, ConcurrentLinkedQueue[Standard]] = {
    val accumulator = new StandardAccumulator
    accumulator.value.addAll(this.standardQueue)
    accumulator
  }

  /**
   * driver端执行有效，用于清空累加器
   */
  override def reset(): Unit = this.standardQueue.clear()

  /**
   * 将新的代码标准化检测结果添加到累加器中
   */
  override def add(v: Standard): Unit = {
    if (FireFrameworkConf.accEnable && v != null) {
      this.standardQueue.add(v)
    }
  }

  /**
   * executor端向driver端merge累加数据
   */
  override def merge(other: AccumulatorV2[Standard, ConcurrentLinkedQueue[Standard]]): Unit = {
    if (other != null && other.value != null && !other.value.isEmpty) {
      this.standardQueue.addAll(other.value)
    }
  }

  /**
   * driver端获取累加器的值
   */
  override def value: ConcurrentLinkedQueue[Standard] = this.standardQueue
}
