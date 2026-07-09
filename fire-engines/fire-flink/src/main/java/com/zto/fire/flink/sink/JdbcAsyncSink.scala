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
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.jdbc.conf.FireJdbcConf

/**
 * Flink jdbc sink组件，底层基于JdbcConnector。
 *
 * 当 `threadNum=1` 时，使用原有单线程批量写逻辑；
 * 当 `threadNum>1` 时，使用 `JdbcConnector.updateBatchAsync` 进行并发分组写入。
 *
 * 调用示例：
 * {{{
 *   stream.addSinkWrap(new JdbcAsyncSink[Student](
 *     sql = "insert into student(name, age) values(?, ?)",
 *     batch = 200,
 *     flushInterval = 1000,
 *     threadNum = 4,
 *     keyNum = 1
 *   ) {
 *     override def map(value: Student): Seq[Any] = Seq(value.name, value.age)
 *   })
 * }}}
 *
 * @author ChengLong 2020-05-22 10:37
 * @since 1.1.0
 */
abstract class JdbcAsyncSink[IN](sql: String,
                                 batch: Int = 100,
                                 flushInterval: Long = 5000,
                                 threadNum: Int = 1,
                                 keyNum: Int = KeyNum._1) extends JdbcSink[IN](sql, batch, flushInterval, keyNum) {

  require(threadNum > 0, s"JdbcSink线程数必须大于0，当前值：$threadNum")

  /**
   * 将数据sink到jdbc
   * 该方法会被flush方法自动调用。
   * 当 `threadNum > 1` 时，每次flush会将当前批次数据拆分后并发写入数据库。
   */
  override def sink(dataList: List[Seq[Any]]): Unit = {
    if (threadNum > 1) {
      JdbcConnector.updateBatchAsync(sql, dataList, threadNum = math.min(threadNum, FireJdbcConf.maxPoolSize(keyNum)), keyNum = keyNum)
    } else {
      JdbcConnector.updateBatch(sql, dataList, keyNum = keyNum)
    }
  }
}
