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
import com.zto.fire.predef._

/**
 * Trace启动器，可根据不同的引擎初始化不同的Trace启动器实例
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] object TraceDynamicLauncher extends TraceLauncher {
  private lazy val launcher: TraceLauncher = this.install

  /**
   * 根据不同的引擎初始化对应的Trace启动器
   */
  private[this] def install: TraceLauncher = {
    val launcher = FireFrameworkConf.traceLauncher
    requireNonEmpty(launcher)(s"Trace启动器不能为空，请通过${FireFrameworkConf.FIRE_TRACE_LAUNCHER}进行配置")
    logWarning(s"Trace启动器${launcher}初始化成功！")
    Class.forName(launcher).newInstance().asInstanceOf[TraceLauncher]
  }

  /**
   * 热启动代码增强
   */
  override def codeTraceStart(isDistribute: Boolean, ip: String, className: String, thresholdMs: Long): Unit =
    this.launcher.codeTraceStart(isDistribute, ip, className, thresholdMs)

  /**
   * 热关闭代码增强
   */
  override def codeTraceStop(isDistribute: Boolean, ip: String): Unit =
    this.launcher.codeTraceStop(isDistribute, ip)

  /**
   * 热重启代码增强
   */
  override def codeTraceRestart(isDistribute: Boolean, ip: String, className: String, thresholdMs: Long): Unit =
    this.launcher.codeTraceRestart(isDistribute, ip, className, thresholdMs)
}
