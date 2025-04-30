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
import com.zto.fire.common.util.{FireUtils, OSUtils, YarnContainerMemoryMonitor, YarnUtils}

/**
 * 内存诊断分析
 *
 * @author ChengLong
 * @Date 2025/4/28 15:12
 * @version 2.5.4
 */
object MemoryAnalysis {
  // yarn container监控组件
  private[this] lazy val containerMemoryMonitor = new YarnContainerMemoryMonitor(FireFrameworkConf.containerMonitorInterval, YarnUtils.getContainerPmemLimit(FireFrameworkConf.containerMonitorPmemRatio), YarnUtils.getContainerVmemLimit(FireFrameworkConf.containerMonitorVmemRatio))

  /**
   * 启动内存监控
   *
   * @param pid
   * 进程的pid
   */
  def startMemoryMonitor(pid: String = OSUtils.getPid): Unit = {
    if (FireFrameworkConf.containerMonitorEnable && OSUtils.isLinux && !FireUtils.isLocalRunMode) {
      this.containerMemoryMonitor.startMonitoring(pid)
    }
  }
}
