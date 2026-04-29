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
import com.zto.fire.core.bean.TraceParam
import com.zto.fire.predef._

/**
 * Trace启动器
 *
 * @author ChengLong
 * @since 3.0.0
 */
private[fire] trait TraceLauncher extends Logging {

  /**
   * 统一管理，用于执行start、stop、restart等命令
   *
   * @param param 用于封装Trace相关命令的参数
   */
  def command(param: TraceParam): Unit = {
    requireNonEmpty(param, param.getCommand)("Trace管理命令不能为空，请检查")
    val isDistribute = if (param.getDistribute == null) false else param.getDistribute.booleanValue()

    param.getCommand match {
      case "start" => this.codeTraceStart(isDistribute, param.getIp, param.getClassName, param.getElapse)
      case "stop" => this.codeTraceStop(isDistribute, param.getIp)
      case "restart" => this.codeTraceRestart(isDistribute, param.getIp, param.getClassName, param.getElapse)
    }
  }

  /**
   * 热启动代码增强
   */
  def codeTraceStart(isDistribute: Boolean, ip: String, className: String, elapse: java.lang.Long): Unit

  /**
   * 热关闭代码增强
   */
  def codeTraceStop(isDistribute: Boolean, ip: String): Unit

  /**
   * 热重启代码增强
   */
  def codeTraceRestart(isDistribute: Boolean, ip: String, className: String, elapse: java.lang.Long): Unit
}
