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

package com.zto.fire.core.plugin

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.core.bean.TracePerformanceTarget
import com.zto.fire.predef._

import java.util

/**
 * Trace performance启动器，可根据不同的引擎初始化不同的Trace performance启动器实例
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] object TracePerformanceDynamicLauncher extends TracePerformanceLauncher {
  private lazy val launcher: TracePerformanceLauncher = this.install

  /**
   * 根据不同的引擎初始化对应的Trace performance启动器
   */
  private[this] def install: TracePerformanceLauncher = {
    val launcher = FireFrameworkConf.traceLauncher
    requireNonEmpty(launcher)(s"Trace performance启动器不能为空，请通过${FireFrameworkConf.FIRE_TRACE_LAUNCHER}进行配置")
    logWarning(s"Trace performance启动器${launcher}初始化成功！")
    Class.forName(launcher).newInstance().asInstanceOf[TracePerformanceLauncher]
  }

  /**
   * 热启动性能代码增强
   */
  override def tracePerformanceStart(isDistribute: Boolean, ip: String, targets: util.List[TracePerformanceTarget]): Unit = {
    this.launcher.tracePerformanceStart(isDistribute, ip, targets)
  }

  /**
   * 热关闭性能代码增强
   */
  override def tracePerformanceStop(isDistribute: Boolean, ip: String): Unit = {
    this.launcher.tracePerformanceStop(isDistribute, ip)
  }

  /**
   * 热重启性能代码增强
   */
  override def tracePerformanceRestart(isDistribute: Boolean, ip: String, targets: util.List[TracePerformanceTarget]): Unit = {
    this.launcher.tracePerformanceRestart(isDistribute, ip, targets)
  }
}
