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

package com.zto.fire.spark.plugin

import com.zto.fire.common.util.{Logging, PropUtils}
import com.zto.fire.core.plugin.ArthasManager
import com.zto.fire.spark.sync.DistributeSyncManager
import com.zto.fire.spark.util.SparkUtils

/**
 * Arthas启动器
 *
 * @author ChengLong 2021-11-3 15:38:20
 * @since 2.2.0
 */
object DymnicArthasLauncher extends Logging {

  /**
   * 热启动Arthas
   *
   * @param isDistribute
   * 是否在每个container端启动arthas
   */
  def hotStartArthas(isDistribute: Boolean): Unit = {
    ArthasManager.startArthas(PropUtils.getString("driver.class.name"), SparkUtils.getExecutorId)
    if (isDistribute) {
      DistributeSyncManager.sync({
        ArthasManager.startArthas(PropUtils.getString("driver.class.name"), s"container_${SparkUtils.getExecutorId}")
      })
    }
  }

  /**
   * 分布式热关闭Arthas相关服务
   *
   * @param isDistribute
   * 是否在每个container端停止arthas
   */
  def hotStopArthas(isDistribute: Boolean): Unit = {
    ArthasManager.stopArthas
    if (isDistribute) {
      DistributeSyncManager.sync({
        ArthasManager.stopArthas
      })
    }
  }

  /**
   * 分布式热重启rthas相关服务
   *
   * @param isDistribute
   * 是否在每个container端停止arthas
   */
  def hotRestartArthas(isDistribute: Boolean): Unit = {
    ArthasManager.restartArthas(PropUtils.getString("driver.class.name"), SparkUtils.getExecutorId)
    if (isDistribute) {
      DistributeSyncManager.sync({
        ArthasManager.restartArthas(PropUtils.getString("driver.class.name"), SparkUtils.getExecutorId)
      })
    }
  }
}
