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

import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream

/**
 * 社区版本：用于包装flink addSink api，实现血缘采集
 *
 * @author ChengLong
 * @Date 2024/3/8 16:51
 * @version 2.4.3
 */
abstract class DataStreamHelperImpl[T](stream: DataStream[T]) extends DataStreamHelper[T](stream) {

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   *
   */
  override protected[fire] def addSinkWrap(sinkFunction: SinkFunction[T]): DataStreamSink[T] = {
    this.stream.$sink(sinkFunction)
  }

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   *
   */
  override protected[fire] def addSinkWrap(fun: T => Unit): DataStreamSink[T] = {
    this.stream.$sink(fun)
  }
}
