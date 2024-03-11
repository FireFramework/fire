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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * ZTO版本：用于包装flink addSource与addSink api，实现血缘采集
 *
 * @author ChengLong
 * @Date 2024/3/8 16:51
 * @version 2.4.3
 */
class StreamExecutionEnvHelperImpl(env: StreamExecutionEnvironment) extends StreamExecutionEnvHelper(env) {

  /**
   * Create a DataStream using a user defined source function for arbitrary
   * source functionality. By default sources have a parallelism of 1.
   * To enable parallel execution, the user defined source should implement
   * ParallelSourceFunction or extend RichParallelSourceFunction.
   * In these cases the resulting source will have the parallelism of the environment.
   * To change this afterwards call DataStreamSource.setParallelism(int)
   *
   */
  override protected[fire] def addSourceWrap[T: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
    this.env.$source[T](function)
  }

  /**
   * Create a DataStream using a user defined source function for arbitrary
   * source functionality.
   */
  override protected[fire] def addSourceWrap[T: TypeInformation](function: SourceContext[T] => Unit): DataStream[T] = {
    this.env.$source[T](function)
  }
}
