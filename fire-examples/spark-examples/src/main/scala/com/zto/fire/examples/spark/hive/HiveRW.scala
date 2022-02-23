package com.zto.fire.examples.spark.hive

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkStreaming
import org.apache.spark.sql.{DataFrame, SaveMode}


/**
 * 基于Fire进行Spark Streaming开发
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    | #注释信息
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire
    |fire.rest.filter.enable=true
    |hive.cluster=test
    |""")
object HiveRW extends BaseSparkStreaming {

  // 消息格式
  // {"age":16,"className":"Student","createTime":"2020-08-03 17:23:05","id":6,"length":15.0,"name":"root","sex":true}
  // {"age":16,"className":"Student","createTime":"2020-08-03 17:23:05","id":6,"length":15.0,"name":"root","sex":true}
  override def process: Unit = {
    this.streaming
    // this.batch
  }

  /**
   * spark core模式
   */
  def batch: Unit = {
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    insert(df)
  }

  /**
   * streaming模式
   */
  def streaming: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.map(t => JSONUtils.parseObject[Student](t.value())).foreachRDD(rdd => {
      val df = this.fire.createDataFrame(rdd, classOf[Student])
      insert(df)
    })
    this.fire.start
  }

  /**
   * 动态分区写入
   */
  def insert(df: DataFrame): Unit = {
    this.fire.sql("set hive.exec.dynamic.partition = true")
    this.fire.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    df.createOrReplaceTempView("t_student")

    this.fire.sql(
      """
        |insert overwrite table tmp.baseorganize_fire
        |select
        | id,
        | name,
        | age,
        | '20220221' as ds
        |from t_student
        |""".stripMargin)

    this.fire.sql(
      """
        |select
        | *,
        | count(1) over()
        |from tmp.baseorganize_fire
        |""".stripMargin).show(3, false)
  }


  override def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
