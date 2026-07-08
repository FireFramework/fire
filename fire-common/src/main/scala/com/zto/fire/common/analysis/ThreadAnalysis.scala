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

package com.zto.fire.common.analysis

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.{FireUtils, Logging, OSUtils, ThreadUtils}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 线程卡死分析入口，在 Flink TaskManager 等 container 端启动周期性检测任务
 *
 * @author ChengLong 2026-07-07 10:26:12
 * @since fire-3.0.0
 */
object ThreadAnalysis extends Logging {
  private val monitor = new ThreadStuckMonitor()
  private val started = new AtomicBoolean(false)

  /**
   * 启动线程卡死监控（幂等）
   *
   * 仅在 Linux 非 local 模式且 fire.analysis.thread.stuck.enable=true 时生效
   * 检测周期由 fire.analysis.thread.stuck.interval 控制
   */
  def startThreadStuckMonitor(): Unit = {
    if (!FireFrameworkConf.threadStuckEnable || !OSUtils.isLinux || FireUtils.isLocalRunMode) {
      return
    }

    if (!started.compareAndSet(false, true)) {
      return
    }

    val intervalMs = FireFrameworkConf.threadStuckIntervalMs
    ThreadUtils.scheduleWithFixedDelay(() => monitor.check(), intervalMs, intervalMs, TimeUnit.MILLISECONDS)
    logWarning(s"Fire Thread stuck monitor started, interval=${intervalMs}ms")
  }
}
