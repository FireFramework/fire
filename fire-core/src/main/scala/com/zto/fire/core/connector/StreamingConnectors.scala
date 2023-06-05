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

package com.zto.fire.core.connector

import com.zto.fire.common.bean.Generator
import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.common.util.{JSONUtils, ReflectionUtils, ThreadUtils}
import org.apache.spark.storage.StorageLevel

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
trait StreamingConnectors {

  /**
   * 随机数生成器
   */
  protected lazy val random = new Random

  /**
   * 随机生成UUID
   */
  protected def genUUID: String = UUID.randomUUID().toString

  /**
   * 随机生成long类型数据
   */
  protected def genRandomLong(abs: Boolean = true): Long = {
    val value = random.nextLong()
    if (abs) Math.abs(value) else value
  }

  /**
   * 随机生成Int型数据
   */
  def genRandomInt(abs: Boolean = true): Int = {
    val value = random.nextInt()
    if (abs) Math.abs(value) else value
  }

  /**
   * 随机生成Double型数据
   */
  protected def genRandomDouble(abs: Boolean = true): Double = {
    val value = random.nextDouble()
    if (abs) Math.abs(value) else value
  }

  /**
   * 随机生成Float型数据
   */
  protected def genRandomFloat(abs: Boolean = true): Float = {
    val value = random.nextFloat()
    if (abs) Math.abs(value) else value
  }

}