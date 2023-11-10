package com.zto.fire.examples.flink.lineage

import com.zto.fire._
import com.zto.fire.common.util.{JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.core.anno.lifecycle.{Step1, Step2, Step3, Step4, Step5}
import com.zto.fire.examples.flink.Test.sql
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
@Hive("fat")
@Streaming(interval = 60, parallelism = 2, unaligned = false)
object FlinkSqlLineageTest extends FlinkStreaming {

  @Step1("血缘信息输出")
  def lineage: Unit = {
    // 定义hive表前先切换到hive catalog
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue) + "\n\n")
    }, 0, 10, TimeUnit.SECONDS)
  }

  /**
   * filesystem
   * datagen
   * hudi
   * hbase
   * hive
   * rocketmq
   * cdc
   * blackhole
   * doris
   * iceberg
   * paimon
   * dynamodb
   * kinesis
   * clickhouse
   * jdbc
   * elasticsearch
   * hello
   * opensearch
   * mongodb
   * print
   */
  @Step2("所有connector血缘")
  def connector(): Unit = {
    // 已知的connector
    sql(
      """
        |CREATE TABLE dim_usr_dept (
        |    id BIGINT
        |  , dept_id BIGINT
        |  , dept_name STRING
        |  , PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |    'connector' = 'jdbc'
        |  , 'url' = 'jdbc:mysql://localhost:3306/db?useSSL=false&characterEncoding=utf8&serverTimezone=Asia/Shanghai'
        |  , 'table-name' = 't_user'
        |  , 'username' = 'root'
        |  , 'password' = '******'
        |)
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE t_kinesis (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `category_id` BIGINT,
        |  `behavior` STRING,
        |  `ts` TIMESTAMP(3)
        |)
        |PARTITIONED BY (user_id, item_id)
        |WITH (
        |  'connector' = 'kinesis',
        |  'stream' = 'user_behavior',
        |  'aws.region' = 'us-east-2',
        |  'scan.stream.initpos' = 'LATEST',
        |  'format' = 'csv'
        |);
        |
        |CREATE TABLE t_firehose (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `category_id` BIGINT,
        |  `behavior` STRING
        |)
        |WITH (
        |  'connector' = 'firehose',
        |  'delivery-stream' = 'user_behavior',
        |  'aws.region' = 'us-east-2',
        |  'format' = 'csv'
        |);
        |
        |CREATE TABLE t_dynamodb (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `category_id` BIGINT,
        |  `behavior` STRING
        |)
        |WITH (
        |  'connector' = 'dynamodb',
        |  'table-name' = 'user_behavior',
        |  'aws.region' = 'us-east-2'
        |);
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE blackhole_table (
        |  f0 INT,
        |  f1 INT,
        |  f2 STRING,
        |  f3 DOUBLE
        |) WITH (
        |  'connector' = 'blackhole'
        |)
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE t_es (
        |  user_id STRING,
        |  user_name STRING,
        |  uv BIGINT,
        |  pv BIGINT,
        |  PRIMARY KEY (user_id) NOT ENFORCED
        |) WITH (
        |  'connector' = 'elasticsearch-7',
        |  'hosts' = 'http://localhost:9200',
        |  'index' = 'users'
        |)
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE t_paimon (
        |    user_id BIGINT,
        |    item_id BIGINT,
        |    behavior STRING,
        |    dt STRING,
        |    hh STRING,
        |    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
        |) PARTITIONED BY (dt, hh) WITH (
        |    'type' = 'paimon',
        |    'warehouse' = 'hdfs:///tmp/test',
        |    'bucket' = '2',
        |    'bucket-key' = 'user_id'
        |);
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE t_filesystem (
        |  column_name1 INT,
        |  column_name2 STRING,
        |  `file.path` STRING NOT NULL METADATA
        |) WITH (
        |  'connector' = 'filesystem',
        |  'path' = 'file:///tmp/whatever',
        |  'format' = 'json'
        |)
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE t_iceberg (
        |    id   BIGINT,
        |    data STRING
        |) WITH (
        |    'connector'='iceberg',
        |    'catalog-name'='hive_prod',
        |    'catalog-database'='hive_db',
        |    'catalog-table'='hive_iceberg_table',
        |    'uri'='thrift://localhost:9083',
        |    'warehouse'='hdfs://nn:8020/path/to/warehouse'
        |)
        |""".stripMargin)

    sql(
      """
        |CREATE table rocket_source (
        |  id int,
        |  name string,
        |  age int,
        |  createTime string
        |) with (
        | 'connector'='fire-rocketmq',
        | 'format'='json',
        | 'rocket.brokers.name'='bigdata_test',
        | 'rocket.topics'='fire',
        | 'rocket.group.id'='fire11',
        | 'rocket.consumer.tag'='*'
        |)
        |""".stripMargin)

    sql(
      """
        |create table if not exists `t_hudi_flink`(
        |  id int,
        |  name string,
        |  age int,
        |  createTime string,
        |  ds string
        |) PARTITIONED BY (`ds`)
        |with(
        |  'connector'='hudi',
        |  'datasource' = 'hudi_test',
        |  'table.type'='MERGE_ON_READ',
        |  'hoodie.datasource.write.recordkey.field'='id',
        |  'precombine.field'='createTime',
        |  'hive_sync.enable'='true',
        |  'hive_sync.db'='hudi',
        |  'hive_sync.table'='t_hudi_flink',
        |  'hive_sync.mode'='hms',
        |  'hoodie.datasource.write.hive_style_partitioning'='true',
        |  'hive_sync.partition_fields'='ds',
        |  'compaction.async.enabled'='false',
        |  'compaction.schedule.enabled'='true',
        |  'compaction.trigger.strategy'='num_commits',
        |  'compaction.delta_commits'='2',
        |  'index.type'='BUCKET',
        |  'hoodie.bucket.index.num.buckets'='2'
        |)
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE t_mongodb (
        |  _id STRING,
        |  name STRING,
        |  age INT,
        |  status BOOLEAN,
        |  PRIMARY KEY (_id) NOT ENFORCED
        |) WITH (
        |   'connector' = 'mongodb',
        |   'uri' = 'mongodb://root:root123@127.0.0.1:27017',
        |   'database' = 'my_db',
        |   'collection' = 'users'
        |);
        |
        |CREATE TABLE t_jdbc (
        |  id BIGINT,
        |  name STRING,
        |  age INT,
        |  status BOOLEAN,
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
        |   'table-name' = 'users'
        |);
        |
        |CREATE TABLE t_opensearch (
        |  user_id STRING,
        |  user_name STRING,
        |  uv BIGINT,
        |  pv BIGINT,
        |  PRIMARY KEY (user_id) NOT ENFORCED
        |) WITH (
        |  'connector' = 'opensearch',
        |  'hosts' = 'http://localhost:9200',
        |  'index' = 'users'
        |);
        |
        |CREATE TABLE t_hbase (
        | rowkey INT,
        | family1 ROW<q1 INT>,
        | family2 ROW<q2 STRING, q3 BIGINT>,
        | family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>,
        | PRIMARY KEY (rowkey) NOT ENFORCED
        |) WITH (
        | 'connector' = 'hbase-1.4',
        | 'table-name' = 'mytable',
        | 'zookeeper.quorum' = 'localhost:2181'
        |);
        |
        |CREATE TABLE t_datagen (
        |    order_number BIGINT,
        |    price        DECIMAL(32,2),
        |    buyer        ROW<first_name STRING, last_name STRING>,
        |    order_time   TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'datagen'
        |);
        |
        |CREATE TABLE t_print (
        |  f0 INT,
        |  f1 INT,
        |  f2 STRING,
        |  f3 DOUBLE
        |) WITH (
        |  'connector' = 'print'
        |);
        |
        |CREATE TABLE t_rocketmq (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `behavior` STRING
        |) WITH (
        |  'connector' = 'rocketmq',
        |  'topic' = 'user_behavior',
        |  'consumerGroup' = 'behavior_consumer_group',
        |  'nameServerAddress' = '127.0.0.1:9876'
        |);
        |
        |CREATE TABLE t_doris (
        |     name STRING,
        |     age INT,
        |     price DECIMAL(5,2),
        |     sale DOUBLE
        |     )
        |     WITH (
        |       'connector' = 'doris',
        |       'fenodes' = 'FE_IP:8030',
        |       'table.identifier' = 'database.table',
        |       'username' = 'root',
        |       'password' = 'password'
        |);
        |
        |CREATE TABLE t_cdc (
        | id INT NOT NULL,
        | name STRING,
        | description STRING,
        | weight DECIMAL(10,3)
        |) WITH (
        | 'connector' = 'mysql-cdc',
        | 'hostname' = 'localhost',
        | 'port' = '3306',
        | 'username' = 'flinkuser',
        | 'password' = 'flinkpw',
        | 'database-name' = 'inventory',
        | 'table-name' = 'products'
        |);
        |
        |CREATE TABLE t_clickhouse (
        |    `id` BIGINT,
        |    `name` STRING,
        |    `age` INT,
        |    `sex` STRING,
        |    `score` DECIMAL,
        |    `createTime` TIMESTAMP
        |) WITH (
        |    'datasource' = 'ck_test',  -- 配置信息在common.properties中，包括url、username、passwd等敏感数据源信息
        |    'database-name' = 'study',
        |    'table-name' = 't_student',
        |    'sink.batch-size' = '10',
        |    'sink.flush-interval' = '3',
        |    'sink.max-retries' = '3'
        |);
        |""".stripMargin)
  }

  @Step3("定义RocketMQ源表")
  def source: Unit = {
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
          |)
          |""".stripMargin)

    // 未知的connector
    sql(
      """
        |CREATE TABLE t_unknown (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT
        |)
        |WITH (
        |  'connector' = 'hello',
        |  'table-name' = 'user_behavior'
        |)
        |""".stripMargin)
    this.fire.useHiveCatalog()
    sql(
      """
        |CREATE TABLE if not exists tmp.t_hive (
        |  id int,
        |  name string,
        |  age int,
        |  createTime string
        |) PARTITIONED BY (ds string) STORED AS orc TBLPROPERTIES (
        | 'partition.time-extractor.timestamp-pattern'='ds',
        | 'sink.partition-commit.trigger'='process-time',
        | 'sink.partition-commit.delay'='1 min',
        | 'sink.partition-commit.policy.kind'='metastore,success-file'
        |)
        |""".stripMargin)
    this.fire.useDefaultCatalog
  }

  @Step4("定义目标表")
  def sink: Unit = {
    sql(
      """
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
        |)
        |""".stripMargin)
  }

  @Step5("数据sink")
  def insert: Unit = {
    sql("""
          |insert into sink select * from source
          |""".stripMargin)

    sql(
      """
        |insert into t_hudi_flink
        |select
        |   id, name, age, createTime, regexp_replace(substr(createTime,0,10),'-','') ds
        |from rocket_source
        |""".stripMargin)
  }
}
