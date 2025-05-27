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

import java.lang.management.{ManagementFactory, ThreadInfo}

/**
 * Jvm工具栏，用于进行线程、内存等的dump
 *
 * @author ChengLong
 * @Date 2025/5/22 16:12
 * @version 2.4.5
 */
object JvmUtils {

  /**
   * 创建线程dump信息
   */
  def createThreadDump: List[ThreadInfo] = {
    val threadMxBean = ManagementFactory.getThreadMXBean
    threadMxBean.dumpAllThreads(true, true).toList
  }
}
