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
import com.zto.fire.core.bean.TraceTarget
import com.zto.fire.core.plugin.{TraceLauncher, TraceManager}
import com.zto.fire.predef._
import com.zto.fire.spark.sync.DistributeSyncManager

import java.util.{List => JList}

/**
 * Spark Trace分布式启动器
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] class SparkTraceLauncher extends TraceLauncher {

  /**
   * 热启动代码增强
   */
  override def codeTraceStart(isDistribute: Boolean, ip: String, targets: JList[TraceTarget]): Unit = {
    TraceManager.startCodeTrace(targets)
    if (isDistribute) {
      DistributeSyncManager.sync({
        if (isEmpty(ip) || ip.contains(OSUtils.getIp)) TraceManager.startCodeTrace(targets)
      })
    }
  }

  /**
   * 热关闭代码增强
   */
  override def codeTraceStop(isDistribute: Boolean, ip: String): Unit = {
    TraceManager.stopCodeTrace()
    if (isDistribute) {
      DistributeSyncManager.sync({
        if (isEmpty(ip) || ip.contains(OSUtils.getIp)) TraceManager.stopCodeTrace()
      })
    }
  }

  /**
   * 热重启代码增强
   */
  override def codeTraceRestart(isDistribute: Boolean, ip: String, targets: JList[TraceTarget]): Unit = {
    TraceManager.restartCodeTrace(targets)
    if (isDistribute) {
      DistributeSyncManager.sync({
        if (isEmpty(ip) || ip.contains(OSUtils.getIp)) TraceManager.restartCodeTrace(targets)
      })
    }
  }
}
