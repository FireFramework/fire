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

package com.zto.fire.flink.sync

import com.zto.fire.common.bean.standard.StandardResult
import com.zto.fire.core.sync.StandardAccumulatorManager

import java.util

/**
 * 用于将各个TaskManager端代码标准化检测结果收集到JobManager端
 *
 * @author ChengLong
 * @since 3.0.0
 */
object FlinkStandardAccumulatorManager extends StandardAccumulatorManager {

  override def add(result: StandardResult): Unit = this.synchronized {
    super.add(result)
  }

  override def add(results: util.Collection[StandardResult]): Unit = this.synchronized {
    super.add(results)
  }
}
