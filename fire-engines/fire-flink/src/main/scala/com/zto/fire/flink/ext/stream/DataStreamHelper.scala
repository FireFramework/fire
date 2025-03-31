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

import com.zto.fire.common.enu.Operation
import com.zto.fire.common.lineage.{DatasourceDesc, LineageManager}
import com.zto.fire.flink.util.FlinkUtils
import com.zto.fire.requireNonNull
import org.apache.flink.api.connector.sink.Sink
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream

/**
 * 用于包装flink addSink api，实现血缘采集
 *
 * @author ChengLong
 * @Date 2024/3/8 16:51
 * @version 2.4.3
 */
abstract class DataStreamHelper[T](stream: DataStream[T]) {

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   *
   */
  protected[fire] def addSinkWrap(sinkFunction: SinkFunction[T]): DataStreamSink[T] = {
    this.stream.addSink(sinkFunction)
  }

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   *
   */
  protected[fire] def addSinkWrap(fun: T => Unit): DataStreamSink[T] = {
    this.stream.addSink(fun)
  }

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   */
  protected[fire] def sinkToWrap(sink: Sink[T, _, _, _]): DataStreamSink[T] = {
    this.stream.sinkTo(sink)
  }

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   */
  protected[fire] def sinkToWrap(sink: org.apache.flink.api.connector.sink2.Sink[T]): DataStreamSink[T] = {
    this.stream.sinkTo(sink)
  }

  /**
   * 添加sink数据源，并维护血缘信息
   */
  def addSinkLineage(sinkFunction: SinkFunction[T])(lineageFun: => Unit): DataStreamSink[T] = {
    lineageFun
    this.addSinkWrap(sinkFunction)
  }

  /**
   * 添加sink数据源，并维护血缘信息
   */
  def addSinkLineage(fun: T => Unit)(lineageFun: => Unit): DataStreamSink[T] = {
    lineageFun
    this.addSinkWrap(fun)
  }

  /**
   * 添加sinkTo数据源，并维护血缘信息
   */
  def sinkToLineage(sink: Sink[T, _, _, _])(lineageFun: => Unit): DataStreamSink[T] = {
    lineageFun
    this.sinkToWrap(sink)
  }

  /**
   * 添加sinkTo数据源，并维护血缘信息
   */
  def sinkToLineage(sink: org.apache.flink.api.connector.sink2.Sink[T])(lineageFun: => Unit): DataStreamSink[T] = {
    lineageFun
    this.sinkToWrap(sink)
  }


  /**
   * 添加sink数据源，并维护血缘信息
   */
  def addSinkLineage2(sinkFunction: SinkFunction[T])(datasourceDesc: DatasourceDesc, operations: Operation*): DataStreamSink[T] = {
    requireNonNull(datasourceDesc, operations)("血缘信息不能为空，请维护血缘信息！")
    LineageManager.addLineage(datasourceDesc, operations: _*)
    this.addSinkWrap(sinkFunction)
  }

  /**
   * 添加sink数据源，并维护血缘信息
   */
  def addSinkLineage2(fun: T => Unit)(datasourceDesc: DatasourceDesc, operations: Operation*): DataStreamSink[T] = {
    requireNonNull(datasourceDesc, operations)("血缘信息不能为空，请维护血缘信息！")
    LineageManager.addLineage(datasourceDesc, operations: _*)
    this.addSinkWrap(fun)
  }

  /**
   * 添加sinkTo数据源，并维护血缘信息
   */
  def sinkToLineage2(sink: Sink[T, _, _, _])(datasourceDesc: DatasourceDesc, operations: Operation*): DataStreamSink[T] = {
    requireNonNull(datasourceDesc, operations)("血缘信息不能为空，请维护血缘信息！")
    LineageManager.addLineage(datasourceDesc, operations: _*)
    this.sinkToWrap(sink)
  }
}
