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
import com.zto.fire.core.anno.connector._
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.anno.Streaming

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |fire.lineage.debug.enable=true
    |spark.sql.extensions=org.apache.spark.sql.TiExtensions
    |spark.tispark.isolation_read_engines=tikv
    |spark.tispark.table.scan_concurrency=1
    |spark.tispark.plan.allow_index_read=false
    |spark.tispark.pd.addresses=ip:2379
    |""")
@HBase("test")
@Hive("fat")
@Streaming(interval = 10)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object Test extends SparkCore {

  override def process: Unit = {
    this.fire.sql(
      """
        |select * from dpa_analysis.ss_chart limit 10000
        |""".stripMargin).createOrReplaceTempView("tidb_view")
    this.fire.sql(
      """
        |insert into tmp.ss_chart_fire select * from tidb_view
        |""".stripMargin)
    this.fire.sql(
      """
        |select * from tmp.ss_chart_fire
        |""".stripMargin).show
  }
}
