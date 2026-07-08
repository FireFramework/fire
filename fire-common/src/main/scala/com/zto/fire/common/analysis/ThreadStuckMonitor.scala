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
import com.zto.fire.common.util.{Logging, ProcessExitUtils, ThreadDumpUtils}
import org.apache.commons.lang3.StringUtils

import java.lang.management.{ManagementFactory, ThreadInfo, ThreadMXBean}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 线程卡死监控器，由 ThreadAnalysis 周期性调用，支持两类检测规则：
 *  1. JVM 死锁：ThreadMXBean.findDeadlockedThreads
 *  2. 特色线程：线程名匹配 + 状态匹配 +（可选）堆栈关键字匹配
 *
 * @author ChengLong 2026-07-08 09:16:19
 * @since fire-3.0.0
 */
class ThreadStuckMonitor extends Serializable with Logging {
  private val threadMxBean: ThreadMXBean = ManagementFactory.getThreadMXBean
  private val namedThreadFirstSeenMs = new ConcurrentHashMap[Long, Long]()
  private val deadlockSignatureFirstSeenMs = new ConcurrentHashMap[String, Long]()
  private val exiting = new AtomicBoolean(false)

  /**
   * 执行一次检测，由外部定时任务调用
   *
   * 单次检测内先执行死锁检测，再执行特色线程检测；任一规则达到退出阈值即终止 JVM
   */
  def check(): Unit = {
    if (!FireFrameworkConf.threadStuckEnable || exiting.get()) {
      return
    }

    try {
      if (FireFrameworkConf.threadStuckDeadlockEnable) {
        // 检测是否存在线程死锁
        checkDeadlockThreads()
      }

      checkHangThreads()
    } catch {
      case t: Throwable =>
        logWarning("Thread stuck monitor check failed", t)
    }
  }

  /**
   * 检测 JVM 级死锁
   *
   * 同一组死锁线程持续存在超过 fire.analysis.thread.stuck.deadlock.exit.delay 后触发退出
   * 死锁解除则清除计时，避免误杀
   */
  private def checkDeadlockThreads(): Unit = {
    val deadlockedIds = threadMxBean.findDeadlockedThreads()
    if (deadlockedIds == null || deadlockedIds.isEmpty) {
      deadlockSignatureFirstSeenMs.clear()
      return
    }

    val signature = buildDeadlockSignature(deadlockedIds)
    val now = System.currentTimeMillis()
    val firstSeen = deadlockSignatureFirstSeenMs.computeIfAbsent(signature, (_: String) => now)
    val stuckMs = now - firstSeen
    val exitDelay = FireFrameworkConf.threadStuckDeadlockExitDelayMs

    threadMxBean.getThreadInfo(deadlockedIds, true, true).foreach { info =>
      if (info != null) {
        logStuckThread(info, "死锁", stuckMs, describeProblem(info, deadlock = true))
      }
    }

    if (stuckMs >= exitDelay) {
      exitJvm(s"检测到 JVM 死锁已持续 $stuckMs ms（阈值 $exitDelay ms），涉及 ${deadlockedIds.length} 个线程")
    }
  }

  /**
   * 检测配置hang住的线程（如 DataStreamer）
   *
   * 仅当线程名、状态、（可选）堆栈均命中时才计时；线程恢复后从 map 中移除，避免短暂波动误报
   */
  private def checkHangThreads(): Unit = {
    val namePatterns = FireFrameworkConf.threadStuckThreadNamePatterns
    if (namePatterns.isEmpty) {
      namedThreadFirstSeenMs.clear()
      return
    }

    val targetStates = FireFrameworkConf.threadStuckThreadStates
    val stackKeywords = FireFrameworkConf.threadStuckThreadStackKeywords
    val exitDelay = FireFrameworkConf.threadStuckThreadExitDelayMs
    val now = System.currentTimeMillis()
    val currentMatchingIds = new java.util.HashSet[Long]()

    threadMxBean.dumpAllThreads(true, true).foreach { info =>
      if (info != null && matchesNamedThread(info, namePatterns, targetStates, stackKeywords)) {
        val threadId = info.getThreadId
        currentMatchingIds.add(threadId)
        val firstSeen = namedThreadFirstSeenMs.computeIfAbsent(threadId, (_: Long) => now)
        val stuckMs = now - firstSeen

        logStuckThread(info, "线程卡死", stuckMs, describeProblem(info, deadlock = false))

        if (stuckMs >= exitDelay) {
          exitJvm(s"检测到卡住线程 [${info.getThreadName}] 处于异常状态已持续 $stuckMs ms（阈值 $exitDelay ms）")
        }
      }
    }

    // 本轮未命中的 threadId 不再保留，线程恢复后重新计时
    namedThreadFirstSeenMs.keySet().retainAll(currentMatchingIds)
  }

  /**
   * 判断线程是否命中hang线程规则
   */
  private def matchesNamedThread(info: ThreadInfo,
                                 namePatterns: Array[String],
                                 targetStates: java.util.Set[Thread.State],
                                 stackKeywords: Array[String]): Boolean = {
    val threadName = info.getThreadName
    val nameMatched = namePatterns.exists { pattern =>
      StringUtils.isNotBlank(pattern) && threadName.contains(pattern.trim)
    }
    if (!nameMatched) {
      return false
    }

    val state = info.getThreadState
    if (!targetStates.isEmpty && !targetStates.contains(state)) {
      return false
    }

    if (stackKeywords.isEmpty) {
      return true
    }

    val stackTrace = info.getStackTrace
    if (stackTrace == null) {
      return false
    }

    stackTrace.exists { element =>
      val frame = element.toString
      stackKeywords.exists { keyword =>
        StringUtils.isNotBlank(keyword) && frame.contains(keyword.trim)
      }
    }
  }

  /**
   * 输出单线程告警日志，包含卡住时长与问题说明
   */
  private def logStuckThread(info: ThreadInfo, category: String, stuckMs: Long, problem: String): Unit = {
    logWarning(
      s"""[ThreadStuckMonitor][$category] thread="${info.getThreadName}" id=${info.getThreadId} state=${info.getThreadState} stuckMs=$stuckMs problem=$problem
         |${ThreadDumpUtils.formatThreadInfo(info)}""".stripMargin)
  }

  /**
   * 打印全量 thread dump 后强制退出 JVM，仅执行一次
   */
  private def exitJvm(reason: String): Unit = {
    if (!exiting.compareAndSet(false, true)) {
      return
    }
    logError(s"[ThreadStuckMonitor] $reason")
    logError(s"[ThreadStuckMonitor] Full thread dump before exit:\n${ThreadDumpUtils.dumpAllThreads(threadMxBean)}")
    ProcessExitUtils.forceExit(1)
  }

  /**
   * 将参与死锁的 threadId 排序后拼接，作为同一死锁组的唯一标识
   */
  private def buildDeadlockSignature(threadIds: Array[Long]): String = {
    threadIds.sorted.mkString(",")
  }

  /**
   * 根据线程状态与堆栈生成可读的问题描述，便于运维快速定位 HDFS / Paimon 等典型卡死场景
   */
  private def describeProblem(info: ThreadInfo, deadlock: Boolean): String = {
    if (deadlock) {
      return "JVM 检测到线程参与循环等待死锁，可能导致 HDFS 写入、Paimon 异步合并或 checkpoint 永久阻塞"
    }

    val state = info.getThreadState
    val stack = joinStack(info.getStackTrace)

    state match {
      case Thread.State.BLOCKED if stack.contains("org.apache.hadoop.hdfs") =>
        "HDFS 相关线程处于 BLOCKED 状态，可能在等待 DataStreamer/DFSOutputStream 内部锁"
      case Thread.State.BLOCKED =>
        "线程处于 BLOCKED 状态，正在等待其他线程释放监视器锁"
      case Thread.State.WAITING | Thread.State.TIMED_WAITING if stack.contains("java.lang.Thread.join") =>
        "线程正在 join 等待其他线程结束，常见于 HDFS DFSOutputStream.close 与 DataStreamer 互相等待"
      case Thread.State.WAITING | Thread.State.TIMED_WAITING if stack.contains("java.util.concurrent.FutureTask.get") =>
        "线程正在等待异步任务完成，常见于 Paimon AsyncPositionOutputStream 关闭流程被阻塞"
      case Thread.State.WAITING | Thread.State.TIMED_WAITING if stack.contains("org.apache.paimon.fs.AsyncPositionOutputStream") =>
        "Paimon 异步输出流关闭被阻塞，可能导致 compaction 或 checkpoint 无法完成"
      case Thread.State.WAITING | Thread.State.TIMED_WAITING if stack.contains("org.apache.hadoop.hdfs") =>
        "HDFS 相关线程长时间等待，可能导致文件写入或租约续期异常"
      case Thread.State.WAITING | Thread.State.TIMED_WAITING =>
        "线程长时间处于等待状态，可能已失去正常推进能力"
      case _ =>
        s"线程命中卡死监控规则，状态=$state"
    }
  }

  private def joinStack(stackTrace: Array[StackTraceElement]): String = {
    if (stackTrace == null || stackTrace.isEmpty) {
      ""
    } else {
      stackTrace.map(_.toString).mkString("\n")
    }
  }
}
