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

package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.bean.lineage.SQLTableColumns
import org.apache.flink.api.scala._
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils, SQLLineageManager, ThreadUtils}
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.{Process, Step1}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.predef.{JString, println}

import java.util.concurrent.TimeUnit

@HBase("test")
@Config("""fire.lineage.run.initialDelay=10""")
@Streaming(interval = 60, unaligned = true, parallelism = 2) // 100s做一次checkpoint，开启非对齐checkpoint
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "1qaz@WSX")
object Test extends FlinkStreaming {
  private val hbaseTable = "fire_test_1"
  private lazy val tableName = "spark_test"

  @Process
  def kafkaSource: Unit = {
    this.sqlLineage
    this.fire.createKafkaDirectStream().print()
    val dstream = this.fire.createRocketMqPullStream()
    dstream.map(t => {
      val timestamp = DateFormatUtils.formatCurrentDateTime()
      val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
      this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
      HBaseConnector.get[Student](hbaseTable, classOf[Student], Seq("1"))
      t
    }).print()
  }

  @Step1("获取血缘信息")
  def lineage: Unit = {
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue))
    }, 0, 60, TimeUnit.SECONDS)
  }

  def sqlLineage: Unit = {
    var dbName = "dw"
    var tableName = "basefire"
    SQLLineageManager.addRelation(tableName, "t_hive_sink")
    SQLLineageManager.setCatalog(dbName, tableName, "hive")
    SQLLineageManager.setTmpView(dbName, tableName, "v_basefire")
    SQLLineageManager.setCluster(dbName, tableName, "localhost:7890")
    SQLLineageManager.setPhysicalTable(dbName, tableName, tableName)
    SQLLineageManager.setColumns(dbName, tableName, new SQLTableColumns("id", "Int"), new SQLTableColumns("name", "String"))
    SQLLineageManager.setOptions(dbName, tableName, Map[String, String]("connector" -> "kafka", "username" -> "root"))
    SQLLineageManager.setOperation(dbName, tableName, "INSERT", "DROP", "CREATE")

    dbName = "ods"
    tableName = "baseuser"
    SQLLineageManager.addRelation(tableName, "t_kafka_sink")
    SQLLineageManager.setCatalog(dbName, tableName, "kafka")
    SQLLineageManager.setTmpView(dbName, tableName, "v_baseuser")
    SQLLineageManager.setCluster(dbName, tableName, "192.168.0.1:7890")
    SQLLineageManager.setPhysicalTable(dbName, tableName, tableName)
    SQLLineageManager.setColumns(dbName, tableName, new SQLTableColumns("id", "Int"), new SQLTableColumns("name", "String"))
    SQLLineageManager.setOptions(dbName, tableName, Map[String, String]("connector" -> "kafka", "username" -> "root"))
    SQLLineageManager.setOperation(dbName, tableName, "INSERT", "DROP", "CREATE")
    println(JSONUtils.toJSONString(SQLLineageManager.getSQLLineage))
  }
}