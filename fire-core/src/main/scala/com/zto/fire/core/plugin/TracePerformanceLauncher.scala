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

import com.zto.fire.common.util.Logging
import com.zto.fire.core.bean.{TracePerformanceParam, TracePerformanceTarget}
import com.zto.fire.predef._

import java.util.{List => JList}

/**
 * Trace performance启动器
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] trait TracePerformanceLauncher extends Logging {

  /**
   * 执行 start / stop / restart；start 与 restart 依赖 `targets`
   *
   * @param param 用于封装Trace performance相关命令的参数
   */
  def command(param: TracePerformanceParam): Unit = {
    requireNonEmpty(param, param.getCommand)("Trace performance管理命令不能为空，请检查")
    val isDistribute = if (param.getDistribute == null) false else param.getDistribute.booleanValue()

    param.getCommand match {
      case "start" =>
        requireNonEmpty(param.getTargets)("Trace performance start 命令需要非空 targets")
        this.tracePerformanceStart(isDistribute, param.getIp, param.getTargets)
      case "stop" => this.tracePerformanceStop(isDistribute, param.getIp)
      case "restart" =>
        requireNonEmpty(param.getTargets)("Trace performance restart 命令需要非空 targets")
        this.tracePerformanceRestart(isDistribute, param.getIp, param.getTargets)
    }
  }

  /**
   * 热启动性能代码增强
   */
  def tracePerformanceStart(isDistribute: Boolean, ip: String, targets: JList[TracePerformanceTarget]): Unit

  /**
   * 热关闭性能代码增强
   */
  def tracePerformanceStop(isDistribute: Boolean, ip: String): Unit

  /**
   * 热重启性能代码增强
   */
  def tracePerformanceRestart(isDistribute: Boolean, ip: String, targets: JList[TracePerformanceTarget]): Unit
}
