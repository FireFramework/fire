package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.alibaba.fastjson.JSON
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
 * flink jdbc sink
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-05-22 11:10
 */
object JdbcSinkTest extends BaseFlinkStreaming {
  lazy val tableName = "spark_test"

  val fields = "name, age, createTime, length, sex".split(",")
  val sql = s"INSERT INTO $tableName (${fields.mkString(",")}) VALUES (?, ?, ?, ?, ?)"

  /**
   * table的jdbc sink
   */
  def testTableJdbcSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select name, age, createTime, length, sex from student group by name, age, createTime, length, sex")

    // 方式一、table中的列顺序和类型需与jdbc sql中的占位符顺序保持一致
    table.jdbcBatchUpdate(sql, keyNum = 3).setParallelism(1)
    // 或者
    this.fire.jdbcBatchUpdateTable(table, sql, keyNum = 3).setParallelism(1)

    // 方式二、自定义row取数规则，适用于row中的列个数和顺序与sql占位符不一致的情况
    table.jdbcBatchUpdate2(sql, flushInterval = 10000, keyNum = 3)(row => {
      Seq(row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4))
    })
    // 或者
    this.flink.jdbcBatchUpdateTable2(table, sql, keyNum = 3)(row => {
      Seq(row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4))
    }).setParallelism(1)
  }

  /**
   * stream jdbc sink
   */
  def testStreamJdbcSink(stream: DataStream[Student]): Unit = {
    // 方式一、指定字段列表，内部根据反射，自动获取DataStream中的数据并填充到sql中的占位符
    // 此处fields有两层含义：1. sql中的字段顺序（对应表） 2. DataStream中的JavaBean字段数据（对应JavaBean）
    // 注：要保证DataStream中字段名称是JavaBean的名称，非表中字段名称 顺序要与占位符顺序一致，个数也要一致
    stream.jdbcBatchUpdate(sql, fields, keyNum = 3).setParallelism(3)
    // 或者
    // this.fire.jdbcBatchUpdateStream(stream, sql, fields).setParallelism(1)

    // 方式二、通过用户指定的匿名函数方式进行数据的组装，适用于上面方法无法反射获取值的情况，适用面更广
    /*stream.jdbcBatchUpdate2(sql, 3, 30000, keyNum = 3) {
      // 在此处指定取数逻辑，定义如何将dstream中每列数据映射到sql中的占位符
      value => Seq(value.getName, value.getAge, DateFormatUtils.formatCurrentDateTime(), value.getLength, value.getSex)
    }.setParallelism(1)*/

    // 或者
    this.flink.jdbcBatchUpdateStream2(stream, sql) {
      value => Seq(value.getName, value.getAge, DateFormatUtils.formatCurrentDateTime(), value.getLength, value.getSex)
    }.setParallelism(2)
  }

  def testJdbc: Unit = {
    // 执行查询操作
    val studentList = this.flink.jdbcQuery(s"select * from $tableName", clazz = classOf[Student], keyNum = 3)
    val dataStream = this.env.fromCollection(studentList)
    dataStream.print()

    // 执行增删改操作
    this.flink.jdbcUpdate(s"delete from $tableName", keyNum = 3)
  }

  override def process: Unit = {
    val stream = this.fire.createKafkaDirectStream().filter(JSONUtils.checkJson _).map(json => JSON.parseObject(json, classOf[Student]))
    // this.testTableJdbcSink(stream)
    this.testStreamJdbcSink(stream)
    this.testJdbc

    this.fire.start("JdbcTest")
  }


  def main(args: Array[String]): Unit = {
    this.init()
  }
}
