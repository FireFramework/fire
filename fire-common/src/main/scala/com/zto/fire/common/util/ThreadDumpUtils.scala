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

import java.lang.management.{ThreadInfo, ThreadMXBean}

/**
 * 线程 dump 格式化工具，输出格式与 jstack 类似，便于日志检索与人工排查
 *
 * @author ChengLong 2026-07-08 14:19:08
 * @since fire-3.0.0
 */
object ThreadDumpUtils {

  /**
   * 导出当前 JVM 全部线程的 dump 文本
   *
   * @param threadMxBean ThreadMXBean 实例
   * @return 多线程 dump 拼接后的字符串，线程之间以换行分隔
   */
  def dumpAllThreads(threadMxBean: ThreadMXBean): String = {
    // lockedMonitors=true, lockedSynchronizers=true，便于分析 BLOCKED / 死锁场景
    threadMxBean.dumpAllThreads(true, true)
      .filter(_ != null)
      .map(this.formatThreadInfo)
      .mkString("\n")
  }

  /**
   * 格式化单个线程的 dump 信息
   *
   * 包含线程名、id、状态、等待锁、堆栈、已持有的 monitor 与 synchronizer
   *
   * @param info ThreadInfo 实例
   * @return 单线程 dump 文本
   */
  def formatThreadInfo(info: ThreadInfo): String = {
    val builder = new StringBuilder(512)

    builder.append('"').append(info.getThreadName).append('"')
      .append(" Id=").append(info.getThreadId)
      .append(' ').append(info.getThreadState)

    val lockName = info.getLockName
    if (lockName != null) {
      builder.append(" on ").append(lockName)
      if (info.getLockOwnerName != null) {
        builder.append(" owned by \"").append(info.getLockOwnerName)
          .append("\" Id=").append(info.getLockOwnerId)
      }
    }

    builder.append('\n')
    info.getStackTrace.foreach { element =>
      builder.append('\t').append("at ").append(element).append('\n')
    }

    info.getLockedMonitors.foreach { monitor =>
      builder.append('\t').append("-  locked ").append(monitor).append('\n')
    }

    val synchronizers = info.getLockedSynchronizers
    if (synchronizers != null && synchronizers.length > 0) {
      builder.append("\tNumber of locked synchronizers = ").append(synchronizers.length).append('\n')
      synchronizers.foreach { synchronizer =>
        builder.append('\t').append("- ").append(synchronizer).append('\n')
      }
    }

    builder.toString
  }
}
