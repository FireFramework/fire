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

package com.zto.fire.examples.spark.thread

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkCore

/**
 * 使用多线程处理数据
 *
 * @author ChengLong
 * @Date 2026/3/12 08:36
 * @version 3.0.0
 */
object ForeachPartitionAsyncTest extends SparkCore {

  override def process(): Unit = {
    // 数据打进一个partition中便于观察测试结果
    val rdd = this.fire.createRDD(Student.newStudentList(), 1)
    println("rdd.count=" + rdd.count)

    // 1. RDD并发API：rdd中9条数据被分割成5分，被5个子线程并发处理
    rdd.foreachPartitionAsync(threadNum = 5)(it => {
      // it集合会被fire框架自动切分成线程数对应的5份，并开启多线程并行计算
      println(s"rdd计算->线程ID：${Thread.currentThread().getId} 数据量=${it.size}")
    })

    // 2. DataFrame并发API：DataFrame中9条数据被分割成3分，被3个子线程并发处理
    val df = this.fire.createDataFrame(rdd, classOf[Student])
    df.foreachPartitionAsync(threadNum = 3)(it => {
      // it集合会被fire框架自动切分成线程数对应的3份，并开启多线程并行计算
      println(s"DF计算->线程ID：${Thread.currentThread().getId} 数据量=${it.size}")
    })

    // 便于观察结果
    Thread.currentThread().join(60000)
  }
}
