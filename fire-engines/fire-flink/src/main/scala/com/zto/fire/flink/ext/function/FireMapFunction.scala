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

package com.zto.fire.flink.ext.function

import com.zto.fire._
import org.apache.flink.api.common.functions._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.io.File
import java.lang
import scala.reflect.ClassTag

/**
 * 增强的MapFunction
 *
 * @tparam I 输入数据类型
 * @tparam O 输出数据类型（map后）
 * @author ChengLong 2021-01-04 09:39:55
 */
@deprecated("use this.getRuntimeContext.getXX", "2.2.0")
abstract class FireMapFunction[I, O] extends AbstractRichFunction with MapFunction[I, O] with MapPartitionFunction[I, O] with FlatMapFunction[I, O] {
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)
  protected lazy val runtimeContext = this.getRuntimeContext()

  /**
   * 根据name获取广播变量
   *
   * @param name 广播变量名称
   * @tparam T
   * 广播变量的类型
   * @return
   * 广播变量引用
   */
  protected def getBroadcastVariable[T](name: String): Seq[T] = {
    requireNonEmpty(name)("广播变量名称不能为空")
    this.runtimeContext.getBroadcastVariable[T](name)
  }

  /**
   * 将值添加到指定的累加器中
   *
   * @param name
   * 累加器名称
   * @param value
   * 待累加的值
   * @tparam T
   * 累加值的类型（Int/Long/Double）
   */
  protected def addCounter[T: ClassTag](name: String, value: T): Unit = {
    requireNonEmpty(name, value)
    getParamType[T] match {
      case valueType if valueType eq classOf[Int] => this.runtimeContext.getIntCounter(name).add(value.asInstanceOf[Int])
      case valueType if valueType eq classOf[Long] => this.runtimeContext.getLongCounter(name).add(value.asInstanceOf[Long])
      case valueType if valueType eq classOf[Double] => this.runtimeContext.getDoubleCounter(name).add(value.asInstanceOf[Double])
    }
  }


  /**
   * 根据文件名获取分布式缓存文件
   *
   * @param fileName 缓存文件名称
   * @return 被缓存的文件
   */
  protected def DistributedCache(fileName: String): File = {
    requireNonEmpty(fileName)("分布式缓存文件名称不能为空！")
    this.runtimeContext.getDistributedCache.getFile(fileName)
  }


  override def map(t: I): O = null.asInstanceOf[O]

  override def mapPartition(iterable: lang.Iterable[I], collector: Collector[O]): Unit = {}

  override def flatMap(t: I, collector: Collector[O]): Unit = {}
}
