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

package com.zto.fire.examples.flink.module

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

/**
 * 线程死锁检测联调示例
 *
 * 在 TaskManager 上故意制造经典交叉加锁死锁，用于验证：
 * fire.analysis.thread.deadlock.* 检测与 exit 逻辑。
 *
 * 使用说明：
 * 1. 需提交到 Linux 上的 Flink 集群（Yarn / K8s 等），local 模式不会启动 ThreadAnalysis
 * 2. 监控由框架 FlinkDistributeExecuteManager 自动启动，示例仅负责制造死锁
 * 3. 观察 TaskManager 日志：Fire Thread analysis monitor started / [ThreadHangMonitor][死锁]
 * 4. 默认仅告警不退出（deadlock.exit.enable=false）
 *
 * @author ChengLong
 * @since fire-3.0.0
 */
@Config(
  """
    |fire.analysis.thread.enable=true
    |fire.analysis.thread.interval=5000
    |fire.analysis.thread.deadlock.enable=true
    |fire.analysis.thread.deadlock.exit.enable=false
    |fire.analysis.thread.deadlock.exit.delay=15000
    |fire.analysis.thread.hang.enable=false
    |""")
@Streaming(interval = 60, parallelism = 1)
object ThreadDeadlockTest extends FlinkStreaming {

  override def process: Unit = {
    this.fire.createRandomIntStream(10)
      .map(new RichMapFunction[Int, Int]() {
        override def open(parameters: Configuration): Unit = {
          DeadlockTriggerMapFunction.triggerOnce()
          println("[ThreadDeadlockTest] deadlock threads started: fire-deadlock-thread-1/2")
        }

        override def map(value: Int): Int = value
      }).print()
  }
}

private object DeadlockTriggerMapFunction {
  private lazy val started = new AtomicBoolean(false)

  /**
   * 经典死锁：线程 1 持 lockA 等 lockB，线程 2 持 lockB 等 lockA
   * 使用 ReentrantLock 便于 ThreadMXBean.findDeadlockedThreads 稳定检出
   */
  def triggerOnce(): Unit = {
    if (!this.started.compareAndSet(false, true)) {
      return
    }

    val lockA = new ReentrantLock()
    val lockB = new ReentrantLock()

    new Thread(new Runnable {
      override def run(): Unit = {
        lockA.lock()
        try {
          Thread.sleep(500)
          lockB.lock()
          try {
          } finally {
            lockB.unlock()
          }
        } finally {
          lockA.unlock()
        }
      }
    }, "fire-deadlock-thread-1").start()

    new Thread(new Runnable {
      override def run(): Unit = {
        lockB.lock()
        try {
          Thread.sleep(500)
          lockA.lock()
          try {
          } finally {
            lockA.unlock()
          }
        } finally {
          lockB.unlock()
        }
      }
    }, "fire-deadlock-thread-2").start()
  }
}
