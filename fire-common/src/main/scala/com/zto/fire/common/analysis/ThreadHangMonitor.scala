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

import com.zto.fire.predef._
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.exception.FireException
import com.zto.fire.common.util.{ExceptionBus, Logging, ProcessExitUtils, ThreadDumpUtils}
import org.apache.commons.lang3.StringUtils

import java.lang.management.{ManagementFactory, ThreadInfo, ThreadMXBean}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 线程诊断监控器，由 ThreadAnalysis 周期性调用，支持两类检测规则：
 *  1. 线程死锁：基于ThreadMXBean进行检测（fire.analysis.thread.deadlock.enable）
 *  2. 夯住线程：线程名匹配 + 状态匹配 +（可选）堆栈关键字匹配（fire.analysis.thread.hang.enable）
 *
 * 检测命中后持续超过对应 exit.delay 时，仅当对应 exit.enable=true 才会退出 JVM；否则仅持续告警
 *
 * @author ChengLong 2026-07-08 09:16:19
 * @since fire-3.0.0
 */
class ThreadHangMonitor extends Serializable with Logging {
  private lazy val threadMxBean: ThreadMXBean = ManagementFactory.getThreadMXBean
  private lazy val hangThreadFirstSeenMs = new JConcurrentHashMap[Long, Long]()
  private lazy val deadlockSignatureFirstSeenMs = new JConcurrentHashMap[String, Long]()
  private lazy val exiting = new AtomicBoolean(false)

  private lazy val threadAnalysisEnable = FireFrameworkConf.threadAnalysisEnable
  private lazy val deadlockEnable = FireFrameworkConf.threadAnalysisDeadlockEnable
  private lazy val deadlockExitEnable = FireFrameworkConf.threadAnalysisDeadlockExitEnable
  private lazy val deadlockExitDelayMs = FireFrameworkConf.threadAnalysisDeadlockExitDelayMs
  private lazy val hangEnable = FireFrameworkConf.threadAnalysisHangEnable
  private lazy val hangNamePatterns = FireFrameworkConf.threadAnalysisHangNamePatterns
  private lazy val hangStates = FireFrameworkConf.threadAnalysisHangStates
  private lazy val hangStackKeywords = FireFrameworkConf.threadAnalysisHangStackKeywords
  private lazy val hangExitEnable = FireFrameworkConf.threadAnalysisHangExitEnable
  private lazy val hangExitDelayMs = FireFrameworkConf.threadAnalysisHangExitDelayMs

  /**
   * 执行一次检测，由外部定时任务调用
   *
   * 单次检测内按开关分别执行死锁检测与夯住线程检测；
   * 任一规则达到退出阈值且对应 exit.enable=true 时终止 JVM
   */
  def check(): Unit = {
    if (!this.threadAnalysisEnable || this.exiting.get()) {
      return
    }

    try {
      if (this.deadlockEnable) {
        // 检测死锁
        this.checkDeadlockThreads()
      }

      if (this.hangEnable) {
        // 检测hang住线程
        this.checkHangThreads()
      } else {
        this.hangThreadFirstSeenMs.clear()
      }
    } catch {
      case t: Throwable =>
        logWarning("Thread hang monitor check failed", t)
    }
  }

  /**
   * 检测 JVM 级线程死锁
   *
   * 同一组死锁线程持续存在超过 fire.analysis.thread.deadlock.exit.delay 后，
   * 若 deadlock.exit.enable=true 则触发退出，否则仅打印日志
   */
  private def checkDeadlockThreads(): Unit = {
    val deadlockedIds = this.threadMxBean.findDeadlockedThreads()
    if (deadlockedIds == null || deadlockedIds.isEmpty) {
      this.deadlockSignatureFirstSeenMs.clear()
      return
    }

    val signature = this.buildDeadlockSignature(deadlockedIds)
    val now = System.currentTimeMillis()
    val firstSeen = this.deadlockSignatureFirstSeenMs.computeIfAbsent(signature, (_: String) => now)
    val durationMs = now - firstSeen

    this.threadMxBean.getThreadInfo(deadlockedIds, true, true).foreach { info =>
      if (info != null) {
        logHangThread(info, "死锁", durationMs, describeProblem(info, deadlock = true))
      }
    }

    if (durationMs >= this.deadlockExitDelayMs) {
      val reason = s"检测到 JVM 死锁已持续 ${elapsed(firstSeen, now)}（阈值 ${this.deadlockExitDelayMs} ms），涉及 ${deadlockedIds.length} 个线程"
      if (this.deadlockExitEnable) {
        shutdownContainer(reason)
      } else {
        logWarning(s"[ThreadHangMonitor] $reason，但 fire.analysis.thread.deadlock.exit.enable=false，仅警告不退出 JVM")
      }
    }
  }

  /**
   * 检测配置的夯住线程（hang，如 DataStreamer）
   *
   * 需同时开启 hang.enable，且配置了 hang.names才进行检测
   * 仅当线程名、状态、（可选）堆栈均命中时才计时；线程恢复后从 map 中移除，避免短暂波动误报
   */
  private def checkHangThreads(): Unit = {
    if (this.hangNamePatterns.isEmpty) {
      this.hangThreadFirstSeenMs.clear()
      return
    }

    val now = System.currentTimeMillis()
    val currentMatchingIds = new JHashSet[Long]()

    this.threadMxBean.dumpAllThreads(true, true).foreach { info =>
      if (info != null && matchesHangThread(info)) {
        val threadId = info.getThreadId
        currentMatchingIds.add(threadId)
        val firstSeen = hangThreadFirstSeenMs.computeIfAbsent(threadId, (_: Long) => now)
        val hangMs = now - firstSeen

        logHangThread(info, "夯住", hangMs, describeProblem(info, deadlock = false))

        if (hangMs >= this.hangExitDelayMs) {
          val reason = s"检测到夯住线程 [${info.getThreadName}] 处于异常状态已持续 $hangMs ms（阈值 ${this.hangExitDelayMs} ms）"
          if (this.hangExitEnable) {
            shutdownContainer(reason)
          } else {
            logWarning(s"[ThreadHangMonitor] $reason，但 fire.analysis.thread.hang.exit.enable=false，仅告警不退出 JVM")
          }
        }
      }
    }

    // 本轮未命中的 threadId 不再保留，线程恢复后重新计时
    this.hangThreadFirstSeenMs.keySet().retainAll(currentMatchingIds)
  }

  /**
   * 判断线程是否命中夯住（hang）规则：匹配线程堆栈中的名称、线程状态以及线程中的关键包+类名
   */
  private def matchesHangThread(info: ThreadInfo): Boolean = {
    val threadName = info.getThreadName

    // 匹配指定的线程名称
    val nameMatched = this.hangNamePatterns.exists { pattern =>
      StringUtils.isNotBlank(pattern) && threadName.contains(pattern.trim)
    }

    if (!nameMatched) {
      return false
    }

    // 匹配指定的线程状态
    val state = info.getThreadState
    if (!this.hangStates.isEmpty && !this.hangStates.contains(state)) {
      return false
    }

    // 匹配线程堆栈中指定的关键字
    if (this.hangStackKeywords.isEmpty) {
      return true
    }

    val stackTrace = info.getStackTrace
    if (stackTrace == null) {
      return false
    }

    stackTrace.exists { element =>
      val frame = element.toString
      this.hangStackKeywords.exists { keyword =>
        StringUtils.isNotBlank(keyword) && frame.contains(keyword.trim)
      }
    }
  }

  /**
   * 输出单线程告警日志，包含持续时长与问题说明
   */
  private def logHangThread(info: ThreadInfo, category: String, durationMs: Long, problem: String): Unit = {
    val deadlockLog = s"""[ThreadHangMonitor][$category] thread="${info.getThreadName}" id=${info.getThreadId} state=${info.getThreadState} durationMs=$durationMs problem=$problem
                 |${ThreadDumpUtils.formatThreadInfo(info)}""".stripMargin
    logWarning(deadlockLog)
    ExceptionBus.post(new FireException(deadlockLog))
  }

  /**
   * 打印全量 thread dump 后强制退出 container，仅执行一次
   */
  private def shutdownContainer(reason: String): Unit = {
    if (!this.exiting.compareAndSet(false, true)) {
      return
    }

    logError(s"[ThreadHangMonitor] $reason")
    logError(s"[ThreadHangMonitor] Full thread dump before exit:\n${ThreadDumpUtils.dumpAllThreads(threadMxBean)}")
    ProcessExitUtils.forceExit(1)
  }

  /**
   * 将参与死锁的 threadId 排序后拼接，作为同一死锁组的唯一标识
   */
  private def buildDeadlockSignature(threadIds: Array[Long]): String = {
    threadIds.sorted.mkString(",")
  }

  /**
   * 根据线程状态与堆栈生成可读的问题描述，便于运维快速定位 HDFS / Paimon 等典型夯住场景
   */
  private def describeProblem(info: ThreadInfo, deadlock: Boolean): String = {
    if (deadlock) {
      return "JVM 检测到线程参与循环等待死锁，可能导致实时任务异常，比如flink checkpoint卡住"
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
        s"线程命中夯住监控规则，状态=$state"
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
