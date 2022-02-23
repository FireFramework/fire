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

package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * 基于Fire进行Flink Streaming开发
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    | #注释信息
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire
    |hive.cluster=test
    |""")
object HiveRW extends BaseFlinkStreaming {

  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    this.fire.useHiveCatalog()

    this.fire.sql(
      """
        |insert into table tmp.baseorganize_fire select * from dim.baseorganize limit 10
        |""".stripMargin)

    this.fire.sql(
      """
        |select * from tmp.baseorganize_fire
        |""".stripMargin).print()
  }
}