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

package com.zto.fire.flink.plugin

import com.zto.fire.common.util.OSUtils
import com.zto.fire.core.bean.TracePerformanceTarget
import com.zto.fire.core.plugin.{TracePerformanceLauncher, TracePerformanceManager}
import com.zto.fire.flink.util.FlinkUtils
import com.zto.fire.predef.{noEmpty, _}

import java.util.{List => JList}

/**
 * Flink Trace performance分布式启动器
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] class FlinkTracePerformanceLauncher extends TracePerformanceLauncher {

  /**
   * 用于判断是否符合执行Trace performance命令的条件
   */
  private[this] def canDo(isDistribute: Boolean, ip: String): Boolean = {
    if (FlinkUtils.isJobManager) return true
    if (isDistribute && FlinkUtils.isTaskManager && (isEmpty(ip) || (noEmpty(ip) && ip.contains(OSUtils.getIp)))) true else false
  }

  /**
   * 热启动性能代码增强
   */
  override def tracePerformanceStart(isDistribute: Boolean, ip: String, targets: JList[TracePerformanceTarget]): Unit = {
    if (this.canDo(isDistribute, ip)) TracePerformanceManager.startTracePerformance(targets)
  }

  /**
   * 热关闭性能代码增强
   */
  override def tracePerformanceStop(isDistribute: Boolean, ip: String): Unit = {
    if (this.canDo(isDistribute, ip)) TracePerformanceManager.stopTracePerformance()
  }

  /**
   * 热重启性能代码增强
   */
  override def tracePerformanceRestart(isDistribute: Boolean, ip: String, targets: JList[TracePerformanceTarget]): Unit = {
    if (this.canDo(isDistribute, ip)) TracePerformanceManager.restartTracePerformance(targets)
  }
}
