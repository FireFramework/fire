package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.hbase.HBaseConnector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable.ListBuffer

/**
 * flink hbase sink
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-5-25 16:32:50
 */
object HBaseSinkTest extends BaseFlinkStreaming {
  lazy val tableName = "fire_test_1"
  lazy val tableName2 = "fire_test_2"
  lazy val tableName3 = "fire_test_3"
  lazy val tableName5 = "fire_test_5"

  /**
   * table的hbase sink
   */
  def testTableHBaseSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.flink.sqlQuery("select id, name, age from student group by id, name, age")
    // 方式一、自动将row转为对应的JavaBean
    // 注意：table对象上调用hbase api，需要指定泛型
    table.hbasePutTable[Student](this.tableName).setParallelism(1)
    this.fire.hbasePutTable[Student](table, this.tableName2, keyNum = 2)

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2[Student](this.tableName3)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.fire.hbasePutTable2[Student](table, this.tableName5, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * table的hbase sink
   */
  def testTableHBaseSink2(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select id, name, age from student group by id, name, age")

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2(this.tableName)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.flink.hbasePutTable2(table, this.tableName2, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink(stream: DataStream[Student]): Unit = {
    // 方式一、DataStream中的数据类型为HBaseBaseBean的子类
    // stream.hbasePutDS(this.tableName)
    this.fire.hbasePutDS[Student](stream, this.tableName)

    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName2, keyNum = 2)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName3)(value => value)
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink2(stream: DataStream[Student]): Unit = {
    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName2, keyNum = 2)(value => value)
  }

  /**
   * hbase的基本操作
   */
  def testHBase: Unit = {
    // get操作
    val getList = ListBuffer(HBaseConnector.buildGet("1"))
    val student = HBaseConnector.get(this.tableName, classOf[Student], getList, 1)
    if (student != null) println(JSON.toJSONString(student, SerializerFeature.NotWriteDefaultValue))
    // scan操作
    val studentList = HBaseConnector.scan(this.tableName, classOf[Student], HBaseConnector.buildScan("0", "9"), 1)
    if (studentList != null) println(JSON.toJSONString(studentList, SerializerFeature.NotWriteDefaultValue))
    // delete操作
    HBaseConnector.deleteRows(this.tableName, Seq("1"))
  }

  override def process: Unit = {
    val stream = this.fire.createKafkaDirectStream().filter(JSONUtils.checkJson(_)).map(json => JSON.parseObject(json, classOf[Student])).setParallelism(1)

    this.testTableHBaseSink(stream)
    // this.testStreamHBaseSink(stream)
    // this.testStreamHBaseSink2(stream)
    // this.testTableHBaseSink2(stream)
    // this.testHBase

    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
