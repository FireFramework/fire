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

package com.zto.fire.common.util

/**
 * JVM 进程退出工具，供线程卡死监控等场景在检测到不可恢复异常后主动终止进程
 *
 * @author ChengLong 2026-07-06 13:26:01
 * @since fire-3.0.0
 */
object ProcessExitUtils extends Logging {

  private val FLINK_SECURITY_MANAGER = "org.apache.flink.core.security.FlinkSecurityManager"

  /**
   * 强制退出当前 JVM 进程。
   *
   * @param exitCode 退出码，非 0 表示异常退出
   */
  def forceExit(exitCode: Int): Unit = {
    logError(s"Fire thread monitor is forcing JVM exit, exitCode=$exitCode")
    try {
      val clazz = Class.forName(FLINK_SECURITY_MANAGER)
      val method = clazz.getMethod("forceProcessExit", classOf[Int])
      method.invoke(null, Int.box(exitCode))
    } catch {
      case e: Throwable =>
        logWarning("FlinkSecurityManager.forceProcessExit unavailable, fallback to Runtime.halt", e)
    }

    // forceProcessExit 正常不应返回；halt 作为最终兜底，避免 shutdown hook 在 hang 场景下再次阻塞
    Runtime.getRuntime.halt(exitCode)
  }
}
