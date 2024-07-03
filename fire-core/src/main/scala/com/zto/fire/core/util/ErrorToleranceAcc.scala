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

package com.zto.fire.core.util

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.ErrorTolerance
import com.zto.fire.common.util.Logging

import java.util.concurrent.atomic.AtomicLong

/**
 * Error tolerance accumulator
 *
 * @author ChengLong
 * @Date 2024/7/3 14:48
 * @version 2.5.0
 */
private[fire] object ErrorToleranceAcc extends Logging {
  private[this] lazy val taskFailedCount = new AtomicLong(0)
  private[this] lazy val stageFailedCount = new AtomicLong(0)
  private[this] lazy val jobFailedCount = new AtomicLong(0)
  private[this] lazy val containerFailedCount = new AtomicLong(0)

  def addTaskFailedCount(count: Long = 1): Long = this.taskFailedCount.addAndGet(count)
  def addStageFailedCount(count: Long = 1): Long = this.stageFailedCount.addAndGet(count)
  def addJobFailedCount(count: Long = 1): Long = this.jobFailedCount.addAndGet(count)
  def addContainerFailedCount(count: Long = 1): Long = this.containerFailedCount.addAndGet(count)

  def getTaskFailedCount: Long = this.taskFailedCount.get()
  def getStageFailedCount: Long = this.stageFailedCount.get()
  def getJobFailedCount: Long = this.jobFailedCount.get()
  def getContainerFailedCount: Long = this.containerFailedCount.get()

  def reset(): Unit = {
    this.taskFailedCount.set(0)
    this.stageFailedCount.set(0)
    this.jobFailedCount.set(0)
    this.containerFailedCount.set(0)
  }

  /**
   * 判断是否需要超过配置的阈值，能否继续执行
   */
  def toFailing: Boolean = {
    val isFailed = FireFrameworkConf.errorToleranceLevel match {
      case ErrorTolerance.NONE => false
      case ErrorTolerance.TASK => this.getTaskFailedCount > FireFrameworkConf.errorToleranceThreshold
      case ErrorTolerance.STAGE => this.getStageFailedCount > FireFrameworkConf.errorToleranceThreshold
      case ErrorTolerance.JOB => this.getJobFailedCount > FireFrameworkConf.errorToleranceThreshold
      case ErrorTolerance.CONTAINER => this.getContainerFailedCount > FireFrameworkConf.errorToleranceThreshold
    }

    if (isFailed) this.showLog()
    isFailed
  }

  /**
   * 记录容错日志
   */
  def showLog(): Unit = {
    logError(
      s"""
        |----------------------容错信息详情------------------------------
        |容错级别：${FireFrameworkConf.errorToleranceLevel}
        |容错阈值：${FireFrameworkConf.errorToleranceThreshold}
        |task失败次数：${this.getTaskFailedCount}
        |stage失败次数：${this.getStageFailedCount}
        |job失败次数：${this.getJobFailedCount}
        |container失败次数：${this.getContainerFailedCount}
        |""".stripMargin)
  }
}
