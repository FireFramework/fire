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

@Hive("fat")
@Config(
  """
    |fire.lineage.run.initialDelay=10
    |fire.lineage.enable=true
    |fire.lineage.collect_sql.enable=true
    |fire.lineage.debug.print=true
    |fire.lineage.debug.enable=true
    |fire.lineage.column.enable=false
    |""")
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
    LineageManager.show(30)
    sql(
      """
        |create table
        |  mobilegatew (
        | appname	    string
        |,file	        string
        |,host	        string
        |,hostname	    string
        |,localip	    string
        |,message	    string
        |,source_type    string
        |,`timestamp`	    string
        |  )
        |with
        |  (
        |    'connector' = 'kafka'
        |  , 'dpa.datasource.name'='mobilegateway'
        |  , 'properties.group.id' = 'mobilegateway_consumer'
        |  , 'scan.startup.mode' = 'latest-offset'
        |  , 'format' = 'json'
        |  );
        |
        |
        |
        |use catalog hive;
        |
        |use catalog `default`;
        |
        |insert into
        |  hive.ods.ods_log_mobilegateway
        |select
        | appname	     as    appname
        |,file	         as    file
        |,host	         as    host
        |,hostname	     as    hostname
        |,localip	     as    localip
        |,message	     as    message
        |,source_type     as    source_type
        |,`timestamp`	     as    `timestamp`
        |,substr(regexp_replace(`timestamp`,'-',''),1,8) as ds
        |from   mobilegatew;
        |""".stripMargin)
  }
}