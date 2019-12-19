package com.zto.fire.demo

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.demo.bean.Student
import com.zto.fire.core.ext.SparkExt._

/**
 * 用于测试scala代码
 *
 * @author ChengLong 2019-9-4 13:39:16
 */
object ScalaTest extends BaseSparkCore {

  override def process: Unit = {
    val df = this.spark.createDataFrame(Student.newStudentList(), classOf[Student])
    val insertSql = s"INSERT INTO spark_test(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"

    // 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序
    // df.jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"))

    df.createOrReplaceTempViewCache("student")
    val sqlDF = this.spark.sql("select name, age, createTime from student where id=100").repartition(1)
    // 若不指定字段，则默认传入当前DataFrame所有列，且列的顺序与sql中问号占位符顺序一致
    sqlDF.jdbcBatchUpdate("insert into spark_test(name, age, createTime) values(?, ?, ?)")
    // 等同以上方式
    // this.spark.jdbcBatchUpdateDF(sqlDF, "insert into spark_test(name, age, createTime) values(?, ?, ?)")
  }

    def main(args: Array[String]): Unit = {
      this.init()
      this.stop
    }

  }
