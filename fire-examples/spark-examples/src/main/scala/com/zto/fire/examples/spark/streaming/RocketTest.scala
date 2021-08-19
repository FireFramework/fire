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

package com.zto.fire.examples.spark.streaming

import com.zto.fire._
import com.zto.fire.spark.BaseSparkStreaming
import com.zto.fire.spark.anno.StreamingDuration

/**
 * 消费rocketmq中的数据
 */
@StreamingDuration(10)
object RocketTest extends BaseSparkStreaming {
  override def process: Unit = {
    //读取RocketMQ消息流
    val dStream = this.fire.createRocketMqPullStream()
    dStream.map(t => new String(t.getBody)).print()

    dStream.rocketCommitOffsets
    this.fire.start()
  }
}
