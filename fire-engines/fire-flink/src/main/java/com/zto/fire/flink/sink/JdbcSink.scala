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

package com.zto.fire.flink.sink

import com.zto.fire.common.conf.KeyNum
import com.zto.fire.predef._
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.jdbc.conf.FireJdbcConf

/**
 * Flink jdbc sink组件，底层基于JdbcConnector
 *
 * @author ChengLong 2020-05-22 10:37
 * @since 1.1.0
 */
abstract class JdbcSink[IN](sql: String,
                            batch: Int = 10,
                            flushInterval: Long = 1000,
                            keyNum: Int = KeyNum._1) extends BaseSink[IN, Seq[Any]](batch, flushInterval) {

  // jdbc操作失败时允许最大重试次数
  this.maxRetry = FireJdbcConf.maxRetry(keyNum)

  /**
   * 将数据sink到jdbc
   * 该方法会被flush方法自动调用
   */
  override def sink(dataList: List[Seq[Any]]): Unit = {
    JdbcConnector.updateBatch(sql, dataList, keyNum = keyNum)
  }
}
