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
import com.zto.fire.common.enu.Operation
import com.zto.fire.common.lineage.{DatasourceDesc, LineageManager}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 用于包装flink addSource与addSink api，实现血缘采集
 *
 * @author ChengLong
 * @Date 2024/3/8 16:51
 * @version 2.4.3
 */
abstract class StreamExecutionEnvHelper(env: StreamExecutionEnvironment) {

  /**
   * Create a DataStream using a user defined source function for arbitrary
   * source functionality. By default sources have a parallelism of 1.
   * To enable parallel execution, the user defined source should implement
   * ParallelSourceFunction or extend RichParallelSourceFunction.
   * In these cases the resulting source will have the parallelism of the environment.
   * To change this afterwards call DataStreamSource.setParallelism(int)
   *
   */
  protected[fire] def addSourceWrap[T: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
    this.env.addSource[T](function)
  }

  /**
   * Create a DataStream using a user defined source function for arbitrary
   * source functionality.
   */
  protected[fire] def addSourceWrap[T: TypeInformation](function: SourceContext[T] => Unit): DataStream[T] = {
    this.env.addSource[T](function)
  }

  /**
   * 添加source数据源的同时维护血缘信息
   */
  def addSourceLineage[T: TypeInformation](function: SourceContext[T] => Unit)(lineageFun: => Unit): DataStream[T] = {
    lineageFun
    this.addSourceWrap[T](function)
  }

  /**
   * 添加source数据源的同时维护血缘信息
   */
  def addSourceLineage[T: TypeInformation](function: SourceFunction[T])(lineageFun: => Unit): DataStream[T] = {
    lineageFun
    this.addSourceWrap[T](function)
  }

  /**
   * 添加source数据源的同时维护血缘信息
   */
  def addSourceLineage2[T: TypeInformation](function: SourceContext[T] => Unit)(datasourceDesc: DatasourceDesc, operations: Operation*): DataStream[T] = {
    requireNonNull(datasourceDesc, operations)("血缘信息不能为空，请维护血缘信息！")
    LineageManager.addLineage(datasourceDesc, operations: _*)
    this.addSourceWrap[T](function)
  }

  /**
   * 添加source数据源的同时维护血缘信息
   */
  def addSourceLineage2[T: TypeInformation](function: SourceFunction[T])(datasourceDesc: DatasourceDesc, operations: Operation*): DataStream[T] = {
    requireNonNull(datasourceDesc, operations)("血缘信息不能为空，请维护血缘信息！")
    LineageManager.addLineage(datasourceDesc, operations: _*)
    this.addSourceWrap[T](function)
  }
}
