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

package com.zto.fire.flink.ext.stream

import com.zto.fire._
import com.zto.fire.common.util.Logging
import org.apache.flink.streaming.api.datastream.DataStreamSink

/**
 * 用于对Flink DataStreamSink的API库扩展
 *
 * @author ChengLong 2023-02-02 13:40:34
 * @since 2.3.3
 */
class DataStreamSinkExt[T](stream: DataStreamSink[T]) extends Logging {

  /**
   * 为当前DataStream设定uid与name
   *
   * @param uid
   * uid
   * @param name
   * name
   * @return
   * 当前实例
   */
  def uname(uid: String, name: String = ""): DataStreamSink[T] = {
    if (noEmpty(uid)) stream.uid(uid)
    if (noEmpty(name)) stream.name(name) else stream.name(uid)
    this.stream
  }
}
