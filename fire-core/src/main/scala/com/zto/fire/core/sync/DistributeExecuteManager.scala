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

package com.zto.fire.core.sync

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.Logging

import java.util.concurrent.atomic.AtomicBoolean

/**
 * 分布式执行管理器：
 * 用于Diver或JobManager端向Executor或TaskManager端的逻辑执行
 *
 * @author ChengLong 2024-04-24 16:57:33
 * @since 2.4.6
 */
private[fire] trait DistributeExecuteManager extends Logging {

  /**
   * 分布式执行相应的逻辑
   */
  protected def distributeExecute: Unit
}

/**
 * 根据不同的计算引擎反射调用对应的实现
 */
private[fire] object DistributeExecuteManagerHelper extends DistributeExecuteManager {
  protected lazy val isRun = new AtomicBoolean(false)

  /**
   * 分布式执行相应的逻辑
   */
  override def distributeExecute: Unit = {
    if (FireFrameworkConf.distributeExecuteEnable && isRun.compareAndSet(false, true)) {
      logInfo("开始执行分布式调用...")
      val execClazz = Class.forName(FireFrameworkConf.distributeExecuteClass)
      val execMethod = execClazz.getDeclaredMethod("distributeExecute")
      execMethod.invoke(null)
      logInfo("完成执行分布式调用...")
    }
  }
}
