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

import com.zto.fire.spark.BaseSparkCore


/**
 * 基于Fire进行Spark Streaming开发
 */
object Test extends BaseSparkCore {

  override def process: Unit = {
    this.spark.sql("use tmp")
    this.spark.sql(
      """
        |alter table tmp.flink_hive_sink add if not exists partition(ds='202106228') location '/user/hive/warehouse/tmp.db/flink_hive_sink/ds=20210628'
        |""".stripMargin)
    this.spark.sql("select * from flink_hive_sink").show(100000, false)
  }
}
