package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.sink.FlinkHBaseSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
 * flink hbase sink
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-5-25 16:32:50
 */
object FlinkHBaseTest extends BaseFlinkStreaming {
  lazy val tableName = "fire_test_1"


  /**
   * table的hbase sink
   */
  def testTableHBaseSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.flink.sql("select id, name, age, createTime, length, sex from student group by id, name, age, createTime, length, sex")
    // 方式一、自动将row转为对应的JavaBean
    table.hbaseOperPutTable(this.tableName, classOf[Student])
    // this.flink.hbaseOperPutTable(table, this.tableName, classOf[Student])

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    // table.hbaseOperPutTable2(this.tableName)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    // this.flink.hbaseOperPutTable2(table, this.tableName)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink(stream: DataStream[Student]): Unit = {
    // 方式一、DataStream中的数据类型为HBaseBaseBean的子类
    stream.hbaseOperPutDS(this.tableName)
    // this.flink.hbaseOperPutDS(stream, this.tableName)

    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbaseOperPutDS2(this.tableName)(value => value)
    // 或者
    // this.flink.hbaseOperPutDS2(stream, this.tableName)(value => value)
  }

  def testHBase: Unit = {
    // 执行查询操作
    val studentList = this.flink.jdbcQuery(s"select * from $tableName", clazz = classOf[Student])
    val dataStream = this.env.fromCollection(studentList)
    dataStream.print()

    // 执行增删改操作
    this.flink.jdbcUpdate(s"delete from $tableName")
  }

  override def process: Unit = {
    val stream = this.ssc.createDirectStream().map(json => JSON.parseObject(json, classOf[Student]))

    this.testTableHBaseSink(stream)
    // this.testStreamHBaseSink(stream)
    // this.testHBase

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
