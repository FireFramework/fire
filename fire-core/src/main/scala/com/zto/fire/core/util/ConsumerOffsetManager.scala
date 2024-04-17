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

import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.JobConsumerInfo
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.{JSONUtils, Logging, MQProducer}

/**
 * 实时计算引擎消费位点管理器
 *
 * @author ChengLong
 * @Date 2024/4/17 13:51
 * @version 2.4.5
 */
@Internal
private[fire] trait ConsumerOffsetManager extends Logging {

  /**
   * 投递消费位点信息到指定消息队列中
   */
  protected def post(consumerInfo: JobConsumerInfo): Unit = {
    if (!FireFrameworkConf.consumerOffsetExportEnable || consumerInfo == null || consumerInfo.getOffsetInfo == null || consumerInfo.getOffsetInfo.isEmpty) return

    val msg = JSONUtils.toJSONString(consumerInfo)
    MQProducer.sendKafka(FireFrameworkConf.consumerOffsetExportMqUrl, FireFrameworkConf.consumerOffsetExportMqTopic, msg)
    logInfo(s"向消息队列投递任务消费位点信息：\n $msg")
  }
}
