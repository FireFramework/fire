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
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.{Process, Step1}
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.{HudiStreaming, SparkStreaming}
import com.zto.fire.spark.anno.Streaming
import com.zto.fire.spark.sync.SparkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  value = """
            |hudi.tableName     =   hudi.t_datacloud
            |hudi.primaryKey    =   id
            |hudi.precombineKey =   createTime
            |hudi.partition     =   ds
            |""", files = Array("hudi-common.properties"))
@Hudi(parallelism = 10, compactCommits = 2, /*clusteringCommits = 3, clustringColumns = "age", */value =
  """
    |hoodie.cleaner.commits.retained=10
    |hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS
    |""")
@HBase("fat")
@Hive("fat")
@Kafka3(brokers = "bigdata_test", topics = "fire", groupId = "fire-123")
@Streaming(interval = 10, concurrent = 2, backpressure = true, maxRatePerPartition = 100)
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@RocketMQ2(brokers = "bigdata_test", topics = "fire", groupId = "fire2")
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "oynZtP#bw7gF8i")
object LineageTest extends HudiStreaming {
  private val hbaseTable = "fire_test_1"
  private lazy val tableName2 = "spark_test"
  val multiPartitionTable = "tmp.mdb_md_dbs_fire_multi_partition_orc"

  /**
   * hudi建表语句，将自动被执行
   * 注：该方法非必须
   */
  override protected def sqlBefore(tableName: String): String = {
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(SparkLineageAccumulatorManager.getValue))
    }, 0, 10, TimeUnit.SECONDS)

    this.source

    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  `id` BIGINT,
       |  `name` STRING,
       |  `age` INT,
       |  `createTime` STRING,
       |  `ds` STRING)
       |USING hudi
       |OPTIONS (
       |  `primaryKey` 'id',
       |  `type` 'mor',
       |  `preCombineField` 'createTime')
       |PARTITIONED BY (ds)
       |""".stripMargin
  }

  /**
   * 将查询语句的结果集插入到指定的hudi表中
   *
   * @param tmpView
   * 每个批次的消息注册成的临时表名
   */
  override protected def sqlUpsert(tmpView: String): String = {
    s"""
       |select
       | id,
       | name,
       | age,
       | createTime,
       | regexp_replace(substr(createTime,0,10),'-','') ds
       |from $tmpView
       |""".stripMargin
  }

  /**
   * 删除hudi数据逻辑，根据指定的id与分区写delete语句或select语句
   * 注：该方法非必须
   *
   * @param tmpView
   * 每个批次的消息注册成的临时表名
   */
  override protected def sqlAfter(tmpView: String): String = {
    s"""
       |delete from ${tableName} where id>90
       |""".stripMargin
  }

  def source: Unit = {
    this.fire.createKafkaDirectStream(keyNum = 3).print()
    sql(
      """
        |CREATE TABLE if not exists `tmp`.`mdb_md_dbs_fire_multi_partition_orc` (
        |  `db_id` BIGINT COMMENT '数据库ID',
        |  `desc` STRING COMMENT '数据库描述',
        |  `db_location_uri` STRING COMMENT '数据库HDFS路径',
        |  `name` STRING COMMENT '数据库名称',
        |  `owner_name` STRING COMMENT 'hive数据库所有者名称',
        |  `owner_type` STRING COMMENT 'hive所有者角色')
        |PARTITIONED BY (ds string, city string)
        |row format delimited fields terminated by '/t';
        |""".stripMargin)
    sql("""set hive.exec.dynamic.partition.mode=nonstrict""")

    sql(
      s"""
         |insert into table ${multiPartitionTable} partition(ds, city) select *,'sh' as city from dw.mdb_md_dbs where ds='20211001' limit 100
         |""".stripMargin)

    val dstream = this.fire.createRocketMqPullStream(keyNum = 2).map(t => JSONUtils.toJSONString(t))
    dstream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val timestamp = DateFormatUtils.formatCurrentDateTime()
        val insertSql = s"INSERT INTO $tableName2 (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
        this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
        HBaseConnector.get[Student](hbaseTable, Seq("1"))
      })

      val studentList = Student.newStudentList()
      val studentDF = this.fire.createDataFrame(studentList, classOf[Student])
      // 每个批次插100条
      studentDF.hbasePutDF[Student](this.hbaseTable)
    })
    dstream.print()
  }

}

