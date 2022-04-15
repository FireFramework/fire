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
import com.zto.fire.spark.BaseSparkCore

/**
 * 基于Fire进行Spark Streaming开发
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire2
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |""")
object Test extends BaseSparkCore {

  override def process: Unit = {
    this.fire.sql(
      """
        |select
        | *
        |from rtdb.zto_ssmx_bill_detail a
        |where
        |a.order_create_date >= '2022-03-23 00:00:00'
        |""".stripMargin).createOrReplaceTempViewCache("t_test")
    this.fire.sql("select * from t_test limit 10").show()
    this.fire.sql(
      """
        |select count(1) from t_test
        |""".stripMargin).show()
  }
}
