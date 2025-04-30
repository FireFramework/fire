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

import com.zto.fire.common.analysis.MemoryAnalysis
import com.zto.fire.common.util.FireUtils
import com.zto.fire.core.sync.DistributeExecuteManager

/**
 * Flink分布式执行器，分布式执行指定的代码逻辑
 *
 * @author ChengLong
 * @Date 2024/4/24 17:00
 * @version 2.4.6
 */
private object FlinkDistributeExecuteManager extends DistributeExecuteManager {

  /**
   * 分布式执行相应的逻辑
   */
  override def distributeExecute: Unit = {
    // 分布式打印指定类的路径信息
    FireUtils.printCodeResource()
    // 启动内存监控工具
    MemoryAnalysis.startMemoryMonitor()
  }
}
