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

package com.zto.fire.core.task

import com.zto.fire.common.bean.runtime.RuntimeInfo
import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import com.zto.fire.common.util.UnitFormatUtils.DateUnitEnum
import com.zto.fire.common.util._
import com.zto.fire.core.BaseFire
import com.zto.fire.predef._
import org.apache.commons.httpclient.Header

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Fire框架内部的定时任务
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-14 11:02
 */
private[fire] abstract class FireInternalTask(baseFire: BaseFire) extends Serializable with Logging {
  private[this] lazy val doJvmMonitor = new AtomicBoolean(true)
  protected lazy val registerLineageHook = new AtomicBoolean(false)

  /**
   * 定时采集运行时的jvm、gc、thread、cpu、memory、disk等信息
   * 并将采集到的数据存放到EnvironmentAccumulator中
   */
  def jvmMonitor: Unit = {
    val runtimeInfo = RuntimeInfo.getRuntimeInfo
    if (runtimeInfo != null && logger != null && this.doJvmMonitor.get) {
      try {
        LogUtils.logStyle(this.logger, s"Jvm信息:${runtimeInfo.getIp}")(logger => {
          val jvmInfo = runtimeInfo.getJvmInfo
          val cpuInfo = runtimeInfo.getCpuInfo
          val threadInfo = runtimeInfo.getThreadInfo
          logger.info(
            s"""${FirePS1Conf.PINK}
               |GC      -> YGC: ${jvmInfo.getMinorGCCount}   YGCT: ${UnitFormatUtils.readable(jvmInfo.getMinorGCTime, UnitFormatUtils.TimeUnitEnum.MS)}    FGC: ${jvmInfo.getFullGCCount}   FGCT: ${UnitFormatUtils.readable(jvmInfo.getFullGCTime, UnitFormatUtils.TimeUnitEnum.MS)}
               |OnHeap  -> Total: ${UnitFormatUtils.readable(jvmInfo.getMemoryTotal, DateUnitEnum.BYTE)}    Used: ${UnitFormatUtils.readable(jvmInfo.getMemoryUsed, DateUnitEnum.BYTE)}   Free: ${UnitFormatUtils.readable(jvmInfo.getMemoryFree, DateUnitEnum.BYTE)}   HeapMax: ${UnitFormatUtils.readable(jvmInfo.getHeapMaxSize, DateUnitEnum.BYTE)}   HeapUsed: ${UnitFormatUtils.readable(jvmInfo.getHeapUseSize, DateUnitEnum.BYTE)}    Committed: ${UnitFormatUtils.readable(jvmInfo.getHeapCommitedSize, DateUnitEnum.BYTE)}
               |OffHeap -> Total: ${UnitFormatUtils.readable(jvmInfo.getNonHeapMaxSize, DateUnitEnum.BYTE)}   Used: ${UnitFormatUtils.readable(jvmInfo.getNonHeapUseSize, DateUnitEnum.BYTE)}   Committed: ${UnitFormatUtils.readable(jvmInfo.getNonHeapCommittedSize, DateUnitEnum.BYTE)}
               |CPUInfo -> Load: ${cpuInfo.getCpuLoad}   LoadAverage: ${cpuInfo.getLoadAverage.mkString(",")}   IoWait: ${cpuInfo.getIoWait}   IrqTick: ${cpuInfo.getIrqTick}
               |Thread  -> Total: ${threadInfo.getTotalCount}    TotalStarted: ${threadInfo.getTotalStartedCount}   Peak: ${threadInfo.getPeakCount}   Deamon: ${threadInfo.getDeamonCount}   CpuTime: ${UnitFormatUtils.readable(threadInfo.getCpuTime, UnitFormatUtils.TimeUnitEnum.MS)}    UserTime: ${UnitFormatUtils.readable(threadInfo.getUserTime, UnitFormatUtils.TimeUnitEnum.MS)} ${FirePS1Conf.DEFAULT}
               |""".stripMargin)
        })
      } catch {
        case _: Throwable => this.doJvmMonitor.set(false)
      }
    }
  }

  /**
   * 实时血缘发送定时任务，定时将血缘信息发送到kafka中
   */
  def lineage: Unit

  /**
   * 注册血缘shutdown hook，确保退出jvm前发送血缘信息到消息队列
   */
  def registerLineageHook(block: => Unit): Unit = {
    if (this.registerLineageHook.compareAndSet(false, true)) {
      ShutdownHookManager.addShutdownHook(ShutdownHookManager.DEFAULT_PRIORITY)(() => block)
    }
  }
}
