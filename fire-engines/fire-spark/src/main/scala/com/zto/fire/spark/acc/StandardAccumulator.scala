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

import com.zto.fire.common.bean.standard.StandardResult
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.Logging
import com.zto.fire.predef._
import org.apache.spark.util.AccumulatorV2

/**
 * Fire框架代码标准化检测累加器，用于将Executor端采集到的不规范API调用汇总到Driver端
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] class StandardAccumulator extends AccumulatorV2[StandardResult, JSet[StandardResult]] with Logging {
  private val standardResults = java.util.Collections.synchronizedSet(new JLinkedHashSet[StandardResult]())

  override def isZero: Boolean = this.standardResults.isEmpty

  override def copy(): AccumulatorV2[StandardResult, JSet[StandardResult]] = {
    val accumulator = new StandardAccumulator
    accumulator.value.addAll(this.standardResults)
    accumulator
  }

  override def reset(): Unit = this.standardResults.clear()

  override def add(v: StandardResult): Unit = {
    if (FireFrameworkConf.accEnable && v != null) {
      this.standardResults.add(v)
    }
  }

  override def merge(other: AccumulatorV2[StandardResult, JSet[StandardResult]]): Unit = {
    if (other != null && other.value != null && !other.value.isEmpty) {
      this.standardResults.addAll(other.value)
    }
  }

  override def value: JSet[StandardResult] = this.standardResults
}
