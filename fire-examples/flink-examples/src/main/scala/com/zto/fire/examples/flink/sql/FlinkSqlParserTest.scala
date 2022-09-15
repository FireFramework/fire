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

package com.zto.fire.examples.flink.sql

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.{JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.core.anno.lifecycle._
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

/**
 * 用于解析flink sql血缘依赖
 *
 * @author ChengLong 2022-09-13 14:20:13
 * @since 2.0.0
 */
@Config(
  """
    |fire.lineage.run.initialDelay=10
    |fire.lineage.run.period=10
    |""")
@Hive("test")
@Streaming(interval = 60, parallelism = 2)
object FlinkSqlParserTest extends FlinkStreaming {
  val hiveTableName = "t_hive_table"
  val select1 =
    """
      |select count(*)
      |from (select * from st.st_fwzl_transfer_kpi_detail_month) a
      |left join (select biz_no,bill_code from dw.dw_kf_center_to_center_dispatch_delay where ds>='20210101') b
      |on a.bill_code=b.bill_code
      |""".stripMargin
  val select2 =
    """
      |select bill_event_id,count(*) from hudi.hudi_bill_item group by bill_event_id
      |""".stripMargin
  val insertInto =
    """
      |insert into ods.base select a,v from tmp.t_user t1 left join ods.test t2 on t1.id=t2.id
      |""".stripMargin
  val renameTable =
    s"""
      |alter table ${hiveTableName} rename to ods.t_user2
      |""".stripMargin
  val dropTable =
    """
      |drop table if exists tmp.test
      |""".stripMargin
  val addPartition =
    s"""
      |alter table ${hiveTableName} add if not exists partition (ds='20210620', city = 'beijing')
      |""".stripMargin
  val renamePartition =
    s"""
       |Alter table ${hiveTableName} partition (ds='201801', city='beijing') rename to partition(ds='202106', city='shanghai')
       |""".stripMargin
  val dropPartition =
    s"""
      |ALTER TABLE ${hiveTableName} DROP IF EXISTS PARTITION (ds='20151219', city = 'beijing')
      |""".stripMargin
  val createHiveTable =
    """
      |CREATE TABLE if not exists `t_hive_table` (
      |  `id` BIGINT,
      |  `name` STRING,
      |  `age` INT,
      |  `createTime` TIMESTAMP,
      |  `length` double
      |) PARTITIONED BY (ds STRING) STORED AS orc TBLPROPERTIES (
      | 'partition.time-extractor.timestamp-pattern'='$ds',
      | 'sink.partition-commit.trigger'='process-time',
      | 'sink.partition-commit.delay'='1 min',
      | 'sink.partition-commit.policy.kind'='metastore,success-file',
      | 'lookup.join.cache.ttl' = '60 s'
      |)
      |""".stripMargin
  val createKafkaTable =
    """
      |CREATE TABLE t_kafka_fire (
      |  `id` BIGINT,
      |  `name` STRING,
      |  `age` INT,
      |  `createTime` TIMESTAMP(3),
      |  `length` double,
      |   proctime as proctime()
      |) WITH (
      |  'datasource' = 'kafka_test',  -- 数据源别名定义在common.properties中，也可通过@Config注解定义
      |  'scan.startup.mode' = 'earliest-offset',
      |  'format' = 'json'
      |)
      |""".stripMargin
  val createRocketMQTable =
    """
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
      |)
      |""".stripMargin
  val createJdbcTable =
    """
      |CREATE TABLE t_mysql_dim (
      |  `id` BIGINT,
      |  `name` STRING,
      |  `ds` STRING,
      |  `count_value` BIGINT,
      |  PRIMARY KEY (id) NOT ENFORCED
      |) WITH (
      |   'datasource' = 'jdbc_test',  -- 数据源别名定义在common.properties中，也可通过@Config注解定义
      |   'table-name' = 't_flink_agg',
      |   'lookup.cache.max-rows'='1000',
      |   'lookup.cache.ttl' = '1h',
      |   'lookup.max-retries' = '3'
      |);
      |""".stripMargin
  val dropDB = "drop database if exists tmp12"
  val insertOverwrite = "insert overwrite table dw.kwang_test partition(ds='202106', city='beijing') values(4,'zz')"

  val insertIntoAsSelect =
    """
      |insert into zto_cockpit_site_target_ds
      |SELECT site_id,scan_date,scan_day,
      |SUM(a.rec_cnt) rec_cnt,
      |SUM(a.order_cnt) order_cnt,
      |SUM(a.disp_cnt) disp_cnt,
      |SUM(a.sign_cnt) sign_cnt,
      |SUM(a.ele_cnt) ele_cnt,
      |SUM(a.bag_cnt) bag_cnt
      |FROM (
      |SELECT t1.site_id,t1.scan_date,t1.scan_day ,
      |t1.cnt rec_cnt,
      |0 order_cnt,
      |0 disp_cnt,
      |0 sign_cnt,
      |t1.ele_cnt ele_cnt,
      |t1.bag_cnt bag_cnt
      |FROM ztkb.zto_cockpit_site_rec_ds t1
      |WHERE t1.scan_day = '#date#'
      |UNION ALL
      |SELECT t2.site_id,t2.order_date scan_date,t2.order_day scan_day ,
      |0 rec_cnt,
      |t2.cnt order_cnt,
      |0 disp_cnt,
      |0 sign_cnt,
      |0 ele_cnt,
      |0 bag_cnt
      |FROM ztkb.zto_cockpit_site_order_ds t2
      |WHERE t2.order_day = '#date#'
      |UNION ALL
      |SELECT t3.site_id,t3.scan_date,t3.scan_day ,
      |0 rec_cnt,
      |0 order_cnt,
      |t3.cnt disp_cnt,
      |0 sign_cnt,
      |0 ele_cnt,
      |0 bag_cnt
      |FROM ztkb.zto_cockpit_site_disp_ds t3
      |WHERE t3.scan_day = '#date#'
      |UNION ALL
      |select t.record_site_id site_id,t.sign_date scan_date,t.sign_day scan_day,
      |0 rec_cnt,
      |0 order_cnt,
      |0 disp_cnt,
      |sum(t.cnt) sign_cnt,
      |0 ele_cnt,
      |0 bag_cnt
      |from ztkb.zto_cockpit_site_sign_ds t
      |where t.sign_day = '#date#'
      |group by t.record_site_id,t.sign_date,t.sign_day
      |) a
      |GROUP BY site_id,scan_date,scan_day
      """.stripMargin

  @Step1("解析Hive SQL")
  def hiveTable: Unit = {
    // 定义hive表前先切换到hive catalog
    this.fire.useHiveCatalog()
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue) + "\n\n")
    }, 0, 10, TimeUnit.SECONDS)
    sql(insertIntoAsSelect)
  }
}
