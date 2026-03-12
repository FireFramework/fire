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
object ConcurrentTest extends SparkCore {

  override def process(): Unit = {
    val studentList = Student.newStudentList()
    val studentDF = this.fire.createDataFrame(studentList, classOf[Student])

    // 每个executor中开启5个线程并发处理迭代器中的数据
    studentDF.foreachPartitionAsync(threadNum = 5) {it => {
      // it这个集合中的数据会在executor端被并发处理
      it.foreach(row => {
        println(s"线程ID：${Thread.currentThread().getId} row=${row}")
      })
    }}

  }
}
