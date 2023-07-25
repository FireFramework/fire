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

import com.zto.fire._
import com.zto.fire.common.util.Logging
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.util.ExceptionUtils

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer

/**
 * Fire框架基础的Flink sink类
 * 提供按批次、固定频率定时flush、checkpoint等功能
 * JDBC sink、HBase sink可继承自此类，并实现自己的flush方法，完成数据的sink
 *
 * @param batch         每批大小，达到该阈值将批量sink到目标组件
 * @param flushInterval 每隔多久刷新一次到目标组件（ms）
 * @author ChengLong 2020-05-21 15:27
 * @since 1.1.0
 */
abstract class BaseSink[IN, OUT](batch: Int, flushInterval: Long) extends RichSinkFunction[IN] with CheckpointedFunction with Logging {
  protected var maxRetry: Long = 3
  private var flushException: Exception = _
  @transient private lazy val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  @transient private var scheduledFuture: ScheduledFuture[_] = _
  protected lazy val closed = new AtomicBoolean(false)
  @transient private lazy val buffer = new ConcurrentLinkedQueue[OUT]()
  @transient private lazy val dataList = ListBuffer[OUT]()
  private lazy val sinkLabel = this.getClass.getSimpleName

  /**
   * 初始化定时调度器，用于定时flush数据到目标组件
   */
  override def open(parameters: Configuration): Unit = {
    require(this.batch > 0, s"${sinkLabel}批次大小必须大于0")
    require(this.maxRetry > 0, s"${sinkLabel}重试次数必须大于0")
    require(this.flushInterval > 0, s"${sinkLabel}刷新频率必须大于0，单位ms")

    this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = this.synchronized {
        if (closed.get()) return
        flush()
      }
    }, this.flushInterval, this.flushInterval, TimeUnit.MILLISECONDS)
  }

  /**
   * 将数据sink到目标组件
   * 不同的组件需定义该flush逻辑实现不同组件的flush操作
   */
  def sink(data: List[OUT]): Unit = {
    // sink逻辑
  }

  /**
   * 将数据构建成sink的格式
   */
  def map(value: IN): OUT

  /**
   * 关闭资源
   * 1. 关闭定时flush线程池
   * 2. 将缓冲区中的数据flush到目标组件
   */
  override def close(): Unit = {
    if (closed.get()) return
    closed.compareAndSet(false, true)

    if (this.scheduledFuture != null) {
      scheduledFuture.cancel(false)
      this.scheduler.shutdown()
    }

    try {
      this.flush()
    } finally {
      super.close()
      this.checkFlushException()
      System.exit(-1)
    }
  }

  /**
   * 将数据sink到缓冲区中
   */
  override def invoke(value: IN, context: SinkFunction.Context): Unit = {
    this.checkFlushException()

    val out = this.map(value)
    if (out != null) this.buffer.offer(out)
    if (this.buffer.size >= this.batch) this.flush()
  }

  /**
   * 内部的flush，调用用户定义的flush方法
   * 并清空缓冲区，将缓冲区大小归零
   */
  def flush(): Unit = this.synchronized {
    this.checkFlushException()

    if (!this.buffer.isEmpty) {
      try {
        retry(this.maxRetry) {
          // 为避免buffer中一直有数据，考虑为循环退出设置一个上限
          while (!this.buffer.isEmpty) {
            val data = this.buffer.poll()
            if (data != null) this.dataList += data
          }
          this.logger.info(s"${sinkLabel}执行flush操作 sink.size=${this.dataList.size} batch=${this.batch} flushInterval=${this.flushInterval}")
          this.sink(this.dataList.toList)
          this.dataList.clear()
        }
      } catch {
        case e: Exception => {
          this.logger.error(s"${sinkLabel}数据sink失败！", e)
          this.flushException = e
        }
      }
    }
  }

  /**
   * checkpoint时将数据全部flush
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    this.flush()
    this.checkFlushException()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    // initializeState
  }

  /**
   * 用于检测在flush过程中是否有异常，如果存在异常，则不再flush
   */
  private def checkFlushException(): Unit = {
    if (flushException != null) {
      ExceptionUtils.stringifyException(flushException)
      throw new RuntimeException(s"${sinkLabel} writing records failed.", flushException)
    }
  }
}
