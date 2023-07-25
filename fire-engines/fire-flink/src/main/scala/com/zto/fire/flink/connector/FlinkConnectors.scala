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

package com.zto.fire.flink.connector

import com.zto.fire.common.bean.Generator
import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.common.util.{JSONUtils, ReflectionUtils, ThreadUtils}
import com.zto.fire.core.connector.StreamingConnectors
import org.apache.flink.runtime.checkpoint.Checkpoint
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.spark.storage.StorageLevel

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.{Random, UUID}
import scala.collection.mutable.ListBuffer
import scala.reflect.{ClassTag, classTag}

/**
 * Flink Streaming自定义source集合
 * 预定义了一些自动生成数据源的source
 *
 * @author ChengLong 2023-06-05 13:47:38
 * @since 2.3.6
 */
object FlinkConnectors extends StreamingConnectors {


  /**
   * 通用的数据产生receiver
   *
   * @param gen
   * 生成数据的函数
   * @param qps
   * 每秒生成的记录数
   * @tparam T
   * 生成消息的格式：DStream[T]
   * @author ChengLong 2023-06-05 08:34:57
   * @since 2.3.6
   */
  class GenConnector[T](gen: => T, qps: Long) extends ParallelSourceFunction[T] with CheckpointedFunction {
    private lazy val isRunning = new AtomicBoolean(false)

    override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
      this.isRunning.compareAndSet(false, true)

      while (this.isRunning.get()) {
        (1L to qps).foreach(_ => sourceContext.collect(gen))
        Thread.sleep(1000)
      }
    }

    /**
     * fire会自动回收线程池，此处无需做任何额外回收动作
     */
    override def cancel(): Unit = {
      this.isRunning.set(false)
    }

    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {}

    override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = {}
  }

  /**
   * UUID connector，随机生成UUID
   *
   * @param qps
   * 每秒生成的记录数
   */
  class UUIDConnector(qps: Long = 1000) extends GenConnector[String](genUUID, qps) {}

  /**
   * Long connector，随机生成Long类型数据
   *
   * @param qps
   * 每秒生成的记录数
   */
  class RandomLongConnector(qps: Long = 1000, abs: Boolean = true) extends GenConnector[Long](genRandomLong(abs), qps) {}

  /**
   * Int connector，随机生成Int类型数据
   *
   * @param qps
   * 每秒生成的记录数
   */
  class RandomIntConnector(qps: Long = 1000, abs: Boolean = true) extends GenConnector[Int](genRandomInt(abs), qps) {}

  /**
   * Double connector，随机生成Double类型数据
   *
   * @param qps
   * 每秒生成的记录数
   */
  class RandomDoubleConnector(qps: Long = 1000, abs: Boolean = true) extends GenConnector[Double](genRandomDouble(abs), qps) {}

  /**
   * Float connector，随机生成Float类型数据
   *
   * @param qps
   * 每秒生成的记录数
   */
  class RandomFloatConnector(qps: Long = 1000, abs: Boolean = true) extends GenConnector[Float](genRandomFloat(abs), qps) {}


  /**
   * 反射调用JavaBean中的generate方法，生成随机的对象实例
   */
  private def genBean[T<: Generator[T] : ClassTag]: T = {
    val clazz = classTag[T].runtimeClass
    val method = ReflectionUtils.getMethodByName(clazz, "generate")
    val instance = clazz.newInstance().asInstanceOf[T]
    method.invoke(instance)
    instance
  }

  /**
   * Java Bean生成connector，JavaBean需继承Generator并实现generate方法
   *
   * @param qps
   * 每秒生成的记录数
   * @param classTag
   * @tparam T
   * 生成消息的格式：DStream[T]
   */
  class BeanConnector[T <: Generator[T] : ClassTag](qps: Long = 1000) extends GenConnector[T](genBean, qps) {}

  /**
   * JSON生成器，JavaBean需继承Generator并实现generate方法
   *
   * @param qps
   * 每秒生成的记录数
   * @param classTag
   * @tparam T
   * 生成消息的格式：DStream[T]
   */
  class JSONConnector[T <: Generator[T] : ClassTag](qps: Long = 1000) extends GenConnector[String](JSONUtils.toJSONString(genBean), qps) {}

}