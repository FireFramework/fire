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

package com.zto.fire.examples.flink.lineage

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.common.lineage.parser.connector.VirtualDatasource
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.{Process, Step1}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager
import com.zto.fire.hbase.HBaseConnector
import org.apache.flink.api.scala._

import java.util.concurrent.TimeUnit

@HBase("fat")
@Config("""fire.lineage.run.initialDelay=10""")
@Streaming(interval = 60, unaligned = true, parallelism = 2) // 100s做一次checkpoint，开启非对齐checkpoint
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@RocketMQ5(brokers = "bigdata_test", topics = "fire5", groupId = "fire5")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka5(brokers = "bigdata_test", topics = "fire5", groupId = "fire5")
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire?useSSL=true", username = "root", password = "root")
@Jdbc5(url = "jdbc:mysql://mysql-server:3306/fire?useSSL=true", username = "root5", password = "root")
object LineageTest extends FlinkStreaming {
  private val hbaseTable = "fire_test_1"
  private lazy val tableName = "spark_test"

  @Process
  def kafkaSource: Unit = {
    this.fire.createKafkaDirectStream().print()
    val dstream = this.fire.createRocketMqPullStream()
    dstream.map(t => {
      val timestamp = DateFormatUtils.formatCurrentDateTime()
      val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
      this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
      HBaseConnector.get[Student](hbaseTable, Seq("1"))
      t
    }).print()

    /*this.fire.fromSourceLineage(source, WatermarkStrategy.noWatermarks(), "kafka") {
      LineageManager.addKafkaLineage(keyNum = 1, Operation.SOURCE)
    }*/

    // 方式一：
    dstream.addSinkLineage(x => println("addSinkLineage.addBlackHoleLineage=>" + x)) {
      LineageManager.addBlackHoleLineage
    }

    dstream.addSinkLineage(x => println("addSinkLineage.kafkaSink=>" + x)) {
      LineageManager.addKafkaLineage(keyNum = 5, Operation.SINK)
    }

    dstream.addSinkLineage(x => println("addSinkLineage.rocketSink=>" + x)) {
      LineageManager.addRocketMQLineage(keyNum = 5, Operation.SINK)
    }

    dstream.addSinkLineage(x => println("addSinkLineage.rocketSink=>" + x)) {
      LineageManager.addTidbLineage("t_user", keyNum = 5, Operation.SINK)
      // 多个数据源
      LineageManager.addMySQLLineage2("jdbc:mysql://mysql-server:3306/fire?useSSL=true", "t_student", "fire", Operation.SELECT, Operation.DELETE, Operation.INSERT)
    }

    // 方式二：
    dstream.addSinkLineage2(x => println("addSinkLineage2=>" + x))(new BlackholeDatasource(Datasource.PRINT.toString), Operation.SINK)
    dstream.addSink(x => println("原生：" + x))

    sql("""
          |CREATE table source (
          |  id int,
          |  name string,
          |  age int,
          |  length double,
          |  data DECIMAL(10, 5)
          |) with (
          | 'connector'='fire-rocketmq',
          | 'format'='json',
          | 'rocket.brokers.name'='bigdata_test',
          | 'rocket.topics'='fire',
          | 'rocket.group.id'='fire',
          | 'rocket.consumer.tag'='*'
          |);
          |
          |CREATE table sink (
          |  id int,
          |  name string,
          |  age int,
          |  length double,
          |  data DECIMAL(10, 5)
          |) with (
          | 'connector'='fire-rocketmq',
          | 'format'='json',
          | 'rocket.brokers.name'='bigdata_test',
          | 'rocket.topics'='fire2',
          | 'rocket.consumer.tag'='*',
          | 'rocket.sink.parallelism'='1'
          |);
          |
          |insert into sink select * from source;
          |""".stripMargin)
  }
}