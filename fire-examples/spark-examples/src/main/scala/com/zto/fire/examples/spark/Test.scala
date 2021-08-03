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

package com.zto.fire.examples.spark

import com.zto.fire.common.anno.Config
import com.zto.fire.spark.BaseSparkCore

/**
 * 基于Fire进行Spark Streaming开发
 */

// 1. 以代码的方式进行配置，支持不单独定义配置文件，如果同时定义了配置文件，则配置文件优先级更高
@Config(props = Array("spark.kafka.topics = test11", "spark.kafka.brokers.name = zms11") , value = Array("test1.properties", "test2.properties"))
// 2. 指定从test.properties加载配置文件
// @Config(Array("test.properties"))
// 3. 指定从以下两个配置文件中加载配置信息
// @Config(Array("test.properties", "test2.properties"))
object Test extends BaseSparkCore {

  override def process: Unit = {
    println("spark.kafka.topics-> " + this.conf.getString("spark.kafka.topics"))
    println("spark.kafka.brokers.name->" + this.conf.getString("spark.kafka.brokers.name"))
  }
}
