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

package com.zto.fire.examples.flink.connector.clickhouse

import com.zto.fire._
import com.zto.fire.common.util.{JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.Kafka
import com.zto.fire.core.anno.lifecycle.{Process, Step1}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Checkpoint
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager
import com.zto.fire.predef.println
import org.apache.flink.api.scala._

import java.util.concurrent.TimeUnit


/**
 * flink clickhouse connector
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Checkpoint(60)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object ClickhouseSinkTest extends FlinkStreaming {

  /**
   * 业务逻辑代码，会被fire自动调用
   */
  @Process
  def handle: Unit = {
    val dstream = this.fire.createKafkaDirectStream().filter(JSONUtils.isJson(_)).map(JSONUtils.parseObject[Student](_))
    dstream.createOrReplaceTempView("t_kafka")
    sql(
      """
        |CREATE TABLE t_user (
        |    `id` BIGINT,
        |    `name` STRING,
        |    `age` INT,
        |    `sex` STRING,
        |    `score` DECIMAL,
        |    `birthday` TIMESTAMP
        |) WITH (
        |    'datasource' = 'ck_test',  -- 配置信息在common.properties中，包括url、username、passwd等敏感数据源信息
        |    'database-name' = 'study',
        |    'table-name' = 't_student',
        |    'sink.batch-size' = '10',
        |    'sink.flush-interval' = '3',
        |    'sink.max-retries' = '3'
        |)
        |""".stripMargin)

    sql(
      """
        |insert into t_user
        |select
        |   id, name, age,
        |   case when sex then '男' else '女' end,
        |   cast(length as DECIMAL),
        |   cast(createTime as TIMESTAMP)
        |from t_kafka
        |""".stripMargin)
  }

  @Step1("获取血缘信息")
  def lineage: Unit = {
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue))
    }, 0, 60, TimeUnit.SECONDS)
  }
}