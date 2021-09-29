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

package com.zto.fire.examples.spark.streaming

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkStreaming
import com.zto.fire.spark.anno.StreamingDuration

/**
 * 消费rocketmq中的数据
 */
@StreamingDuration(10)
@Config(props = Array("rocket.brokers.name=bigdata_test", "rocket.topics=fire", "rocket.group.id=fire", "rocket.consumer.tag=fire", "rocket.starting.offsets=latest", "spark.fire.config_center.enable=false"))
object RocketTest extends BaseSparkStreaming {
  override def process: Unit = {
    //读取RocketMQ消息流
    val dStream = this.fire.createRocketMqPullStream()
    dStream.foreachRDDAtLeastOnce(rdd => {
      val studentRDD = rdd.map(message => new String(message.getBody)).map(t => JSONUtils.parseObject[Student](t)).repartition(2)
      val insertSql = s"INSERT INTO spark_test2(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
      println("rocket.brokers.name=>" + this.conf.getString("rocket.brokers.name"))
      studentRDD.toDF().jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"), batch = 100)
    })(reTry = 5, exitOnFailure = true)
    this.fire.start()
  }
}
