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
import com.zto.fire.common.enu.Operation
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.core.anno.connector._
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |fire.lineage.debug.enable=true
    |""")
@HBase("test")
@Hive("fat")
@Streaming(interval = 10)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object Test extends SparkStreaming {

  override def process: Unit = {
    println("loader=" + this.spark.getClass.getClassLoader)

    val stream = this.fire.createKafkaDirectStream()
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        LineageManager.addPrintLineage(Operation.SINK)
      })
    })
    LineageManager.addMySQLLineage("jdbc://localhost:3306/fire", "t_user", "root", Operation.INSERT_INTO)
    stream.print()
    LineageManager.addSql("""select * from tmp.baseorganize""")
    LineageManager.print(10)
  }

}
