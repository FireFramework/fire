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

package com.zto.fire.examples.spark.lineage

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils}
import com.zto.fire.core.anno.connector.{HBase, Hive, Jdbc, Kafka, RocketMQ}
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.connector.paimon.PaimonStreaming
import org.apache.spark.sql.DataFrame

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Hive(value = "fat")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object SparkCoreLineageTest extends PaimonStreaming {
  override def process: Unit = {
    LineageManager.print(30)
    sql(
      """
        |drop table if exists tmp.paimon_xiaotiantong
        |""".stripMargin)
    val df = sql(
      """
        |create table tmp.paimon_xiaotiantong as select * from paimon.paimon.paimon_xiaotiantong
        |""".stripMargin)
    df.show
    sql(
      """
        |select * from tmp.paimon_xiaotiantong limit 10
        |""".stripMargin).show
    val stream = this.fire.createKafkaDirectStream()
    stream.print()
    /*this.ddl
    // this.streaming
    this.batch*/
  }
}
