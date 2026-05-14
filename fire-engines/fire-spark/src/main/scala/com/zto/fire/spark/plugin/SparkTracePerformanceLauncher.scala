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

package com.zto.fire.spark.plugin

import com.zto.fire.common.util.OSUtils
import com.zto.fire.core.bean.TracePerformanceTarget
import com.zto.fire.core.plugin.{TracePerformanceLauncher, TracePerformanceManager}
import com.zto.fire.predef._
import com.zto.fire.spark.sync.DistributeSyncManager

import java.util.{List => JList}

/**
 * Spark Trace performance分布式启动器
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] class SparkTracePerformanceLauncher extends TracePerformanceLauncher {

  /**
   * 热启动性能代码增强
   */
  override def tracePerformanceStart(isDistribute: Boolean, ip: String, targets: JList[TracePerformanceTarget]): Unit = {
    TracePerformanceManager.startTracePerformance(targets)
    if (isDistribute) {
      DistributeSyncManager.sync({
        if (isEmpty(ip) || ip.contains(OSUtils.getIp)) TracePerformanceManager.startTracePerformance(targets)
      })
    }
  }

  /**
   * 热关闭性能代码增强
   */
  override def tracePerformanceStop(isDistribute: Boolean, ip: String): Unit = {
    TracePerformanceManager.stopTracePerformance()
    if (isDistribute) {
      DistributeSyncManager.sync({
        if (isEmpty(ip) || ip.contains(OSUtils.getIp)) TracePerformanceManager.stopTracePerformance()
      })
    }
  }

  /**
   * 热重启性能代码增强
   */
  override def tracePerformanceRestart(isDistribute: Boolean, ip: String, targets: JList[TracePerformanceTarget]): Unit = {
    TracePerformanceManager.restartTracePerformance(targets)
    if (isDistribute) {
      DistributeSyncManager.sync({
        if (isEmpty(ip) || ip.contains(OSUtils.getIp)) TracePerformanceManager.restartTracePerformance(targets)
      })
    }
  }
}
