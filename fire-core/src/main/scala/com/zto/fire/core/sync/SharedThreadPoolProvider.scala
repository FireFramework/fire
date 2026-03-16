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
import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.common.util.{Logging, ThreadUtils}

import java.util.concurrent.ExecutorService

/**
 * 引擎侧共享线程池提供器：
 * 在每个Executor/TaskManager进程内按资源标识创建并复用managed线程池
 *
 * @author ChengLong 2026-03-16 10:24:13
 * @since 3.0.0
 */
trait SharedThreadPoolProvider extends Logging {
  protected def engineType: String

  /**
   * 资源标识：
   * Spark中通常为executorId，Flink中通常为TaskManager resource-id。
   */
  protected def resourceId: String

  /**
   * 获取共享线程池
   */
  protected[fire] lazy val sharedThreadPool: ExecutorService = this.sharedThreadPool(FireFrameworkConf.sharedThreadPoolSize)

  /**
   * 根据传入的线程数量获取共享线程池
   * 1. 如果线程名相同则自动复用
   * 2. 该线程池由fire框架自动管理生命周期
   */
  private[this] def sharedThreadPool(threadNum: Int): ExecutorService = {
    val poolSize = math.abs(threadNum)
    require(poolSize > 0, s"线程数必须大于0，当前值：$threadNum")
    val poolName = s"FireSharedThreadPool_${engineType}_${resourceId}_${poolSize}"
    ThreadUtils.createManagedThreadPool(poolName, ThreadPoolType.FIXED, poolSize)
  }
}

