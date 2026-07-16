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
 * 线程诊断分析入口，在 Flink TaskManager / Spark Executor 等 container 端启动周期性检测任务
 *
 * @author ChengLong 2026-07-07 10:26:12
 * @since fire-3.0.0
 */
object ThreadAnalysis extends Logging {
  private val monitor = new ThreadHangMonitor()
  private val started = new AtomicBoolean(false)

  /**
   * 启动线程诊断分析监控（幂等）
   *
   * 仅在 Linux 非 local 模式且 fire.analysis.thread.enable=true 时生效。
   * 检测周期由 fire.analysis.thread.interval 控制。
   * 死锁检测、夯住线程（hang）检测及各自是否退出 JVM，分别由对应 enable / exit.enable 参数控制。
   */
  def startThreadAnalysisMonitor(): Unit = {
    if (!FireFrameworkConf.threadAnalysisEnable || !OSUtils.isLinux || FireUtils.isLocalRunMode) {
      return
    }

    if (!started.compareAndSet(false, true)) {
      return
    }

    val intervalMs = FireFrameworkConf.threadAnalysisIntervalMs
    ThreadUtils.scheduleWithFixedDelay({
      this.monitor.check()
    }, intervalMs, intervalMs, TimeUnit.MILLISECONDS)
    logWarning(
      s"Fire Thread analysis monitor started, interval=${intervalMs}ms, " +
        s"deadlock.enable=${FireFrameworkConf.threadAnalysisDeadlockEnable}, " +
        s"deadlock.exit.enable=${FireFrameworkConf.threadAnalysisDeadlockExitEnable}, " +
        s"hang.enable=${FireFrameworkConf.threadAnalysisHangEnable}, " +
        s"hang.exit.enable=${FireFrameworkConf.threadAnalysisHangExitEnable}")
  }
}
