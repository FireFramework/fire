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

package com.zto.fire.core.plugin

import com.taobao.arthas.agent.attach.ArthasAgent
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.{FireUtils, Logging, ThreadUtils}
import com.zto.fire.predef.{JHashMap, _}

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Arthas插件管理
 *
 * @author ChengLong 2021-11-2 14:45:43
 * @since 2.2.0
 */
private[fire] object ArthasManager extends Logging {
  private lazy val start = new AtomicBoolean(false)

  /**
   * 启动Arthas服务
   *
   * @param appName        用于标识任务的名称
   * @param resourceId     用于标识分布式任务的master与slave
   * @param startContainer 是否在分布式环境下启用Arthas
   */
  def startArthas(appName: String, resourceId: String, startContainer: Boolean): Unit = {
    requireNonEmpty(appName, resourceId)("appName或resourceId不能为空，arthas所监控的程序必须有标识！")

    if (resourceId.contains("container") && !startContainer) return
    this.startArthas(appName, resourceId)
  }

  /**
   * 启动Arthas服务
   *
   * @param appName    用于标识任务的名称
   * @param resourceId 用于标识分布式任务的master与slave
   */
  def startArthas(appName: String, resourceId: String): Unit = {
    if (this.start.get()) return

    ThreadUtils.run {
      tryWithLog {
        val configMap = new JHashMap[String, String]()
        configMap.put("arthas.appName", s"${FireUtils.engine}@${appName}")
        configMap.put("arthas.telnetPort", "0")
        configMap.put("arthas.httpPort", "0")
        configMap.put("arthas.agentId", s"${FireUtils.engine}@${appName}_$resourceId")
        configMap.put("arthas.tunnelServer", FireFrameworkConf.arthasTunnelServerUrl)
        configMap.putAll(FireFrameworkConf.arthasConfMap)
        ArthasAgent.attach(configMap)
        this.start.compareAndSet(false, true)
      }(this.logger, tryLog = "<-- Arthas服务已启动 -->")
    }
  }
}
