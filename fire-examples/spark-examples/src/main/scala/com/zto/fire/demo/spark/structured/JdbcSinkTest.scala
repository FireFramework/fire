package com.zto.fire.demo.spark.structured

import com.zto.fire.core.BaseStructuredStreaming
import com.zto.fire.demo.bean.Student
import com.zto.fire.core.ext.SparkExt._

/**
 * 结构化流测试
 */
object JdbcSinkTest extends BaseStructuredStreaming {

  override def process: Unit = {
    // 接入kafka并解析json，支持大小写，默认表名为kafka
    val kafkaDataset = this.spark.loadKafkaParseJson()
    // 直接使用或sql
    /*kafkaDataset.print()
    this.spark.sql("select * from kafka").print()*/

    // jdbc的sql语句
    val insertSql = "insert into spark_test(name, age, createTime, length, sex, rowKey) values(?,?,?,?,?,?)"

    // 将流数据持续写入到关系型数据库中（插入部分列）
    // kafkaDataset.select("data.name", "data.age", "data.createTime", "data.length", "data.sex", "data.rowKey").jdbcBatchUpdate(insertSql)
    // 插入所有列并在Seq中列举DataFrame指定顺序，该顺序必须与insertSql中的问号占位符存在绑定关系
    // kafkaDataset.select("data.*").jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex", "rowKey"))

    this.spark.createDataFrame(Student.newStudentList(), classOf[Student]).createOrReplaceTempViewCache("student")
    this.spark.sql(
      """
        |select
        | t.name,
        | s.length
        |from kafka t left join student s
        | on t.name=s.name
        |""".stripMargin).print()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
