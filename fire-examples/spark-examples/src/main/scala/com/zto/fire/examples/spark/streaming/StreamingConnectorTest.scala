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

package com.zto.fire.examples.spark.streaming

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

import java.util.UUID

/**
 * 基于DataGenReceiver来随机生成测试数据集
 *
 * @author ChengLong 2022-03-07 15:35:55
 * @since 2.2.1
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(10)
object StreamingConnectorTest extends SparkStreaming {

  override def process: Unit = {
    // 创建自定义数据生成规则的流接收器，第一个参数需要传递数据生成规则的函数定义：gen: => T
    val dstream = this.fire.createGenStream(UUID.randomUUID().toString, 1)
    dstream.print(1)

    // 基于JavaBean创建对象流，JavaBean必须实现Generator接口
    val beanStream = this.fire.createBeanStream[Student](1)
    beanStream.print(1)

    // 基于JavaBean创建json流（根据JavaBean序列化为json），JavaBean必须实现Generator接口
    val jsonStream = this.fire.createJSONStream[Student](1)
    jsonStream.print(1)

    // 创建uuid流
    val uuidStream = this.fire.createUUIDStream(qps = 1)
    uuidStream.print(1)

    // 创建Int型随机数流
    val intStream = this.fire.createRandomIntStream(1)
    intStream.print(1)

    // 创建Long型随机数流
    val longStream = this.fire.createRandomLongStream(1)
    longStream.print(1)

    // 创建Double型随机数流
    val doubleStream = this.fire.createRandomDoubleStream(1)
    doubleStream.print(1)

    // 创建Float型随机数流
    val flaotStream = this.fire.createRandomFloatStream(1)
    flaotStream.print(1)
  }
}
