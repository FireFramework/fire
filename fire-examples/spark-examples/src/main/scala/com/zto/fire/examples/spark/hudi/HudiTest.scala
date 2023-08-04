package com.zto.fire.examples.spark.hudi

import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils, MQProducer, ThreadUtils}
import com.zto.fire.core.anno.connector.{Hive, Hudi, Kafka, RocketMQ}
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.spark.lineage.LineageTest.{multiPartitionTable, sql}
import com.zto.fire.spark.HudiStreaming
import com.zto.fire.spark.anno.Streaming
import com.zto.fire.spark.sync.SparkLineageAccumulatorManager

import java.math.BigDecimal
import java.util.concurrent.TimeUnit

@Config(
  value = """
            |hudi.tableName     =   hudi.t_datacloud
            |hudi.primaryKey    =   id
            |hudi.precombineKey =   createTime
            |hudi.partition     =   ds
            |""", files = Array("hudi-common.properties"))
@Hive(cluster = "fat")
@Hudi(parallelism = 10, compactCommits = 2, value =
  """
    |hoodie.cleaner.commits.retained=10
    |hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS
    |""")
@Streaming(interval = 20, backpressure = false, parallelism = 10)
@RocketMQ(brokers = "bigdata_test", topics = "datacloud", groupId = "fire")
object HudiTest extends HudiStreaming {

  /**
   * hudi建表语句，将自动被执行
   * 注：该方法非必须
   */
  override protected def sqlBefore(tableName: String): String = {
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(SparkLineageAccumulatorManager.getValue))
    }, 0, 10, TimeUnit.SECONDS)
    this.sendMsg

    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    df.createOrReplaceTempView("t_student")
    sql(
      """
        |select * from t_student
        |""".stripMargin).show
    sql(
      """
        |create temporary view my_view as select * from t_student
        |""".stripMargin)
    sql(
      s"""
         |create table if not exists tmp.mdb_md_dbs_fire_multi_partition_orc like dw.mdb_md_dbs;
         |
         |insert into table tmp.mdb_md_dbs_fire_multi_partition_orc partition(ds)
         |select
         |    db_id
         |  , desc
         |  , db_location_uri
         |  , name
         |  , owner_name
         |  , owner_type
         |  , ds
         |from
         |    dw.mdb_md_dbs
         |where
         |    ds = '20211125' limit 10
         |""".stripMargin)

    sql(
      s"""
        |drop table if exists $tableName
        |""".stripMargin)

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
    /*sql(
      s"""
        |update $tableName set name='hello' where id=90
        |""".stripMargin)*/
    s"""
       |delete from ${tableName} where id>90
       |""".stripMargin
  }

  def sendMsg: Unit = {
    ThreadUtils.runAsSingle({
      var i = 1
      while (true) {
        val student = new Student(i, s"admin$i", i, BigDecimal.valueOf(i), true, DateFormatUtils.formatCurrentDateTime)
        MQProducer.sendRocketMQ("bigdata_test", "datacloud", JSONUtils.toJSONString(student))
        i += 1
        if (i >= 100) i = 1
        Thread.sleep(1000)
      }
    })
  }
}



