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

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector._
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.anno.Streaming

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |fire.lineage.debug.enable=false
    |fire.debug.class.code.resource=org.apache.spark.sql.SparkSession,com.zto.fire.examples.spark.test.Test,org.apache.hadoop.hbase.client.ConnectionManager
    |""")
@HBase(cluster = "fat", user = "test-sparksubmit")
@Hive("fat")
@Streaming(interval = 10)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object Test extends SparkCore {

  override def process: Unit = {
    val rdd = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    println("count=" + rdd.count())
    Thread.sleep(1000000)
  }
}
