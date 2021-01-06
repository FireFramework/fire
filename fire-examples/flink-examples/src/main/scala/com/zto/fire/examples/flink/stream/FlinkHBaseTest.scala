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
object FlinkHBaseTest extends BaseFlinkStreaming {
  lazy val tableName = "fire_test_1"

  /**
   * table的hbase sink
   */
  def testTableHBaseSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.flink.sql("select id, name, age from student group by id, name, age")
    // 方式一、自动将row转为对应的JavaBean
    // table.hbasePutTable(this.tableName, classOf[Student])
    // this.tableEnv.hbasePutTable(table, this.tableName, classOf[Student], multiVersion = true)

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    // table.hbasePutTable2(this.tableName)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    // this.flink.hbasePutTable2(table, this.tableName)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * table的hbase sink
   */
  def testTableHBaseSink2(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.flink.sql("select id, name, age from student group by id, name, age")

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    // table.hbasePutTable2(this.tableName)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    //this.flink.hbasePutTable2(table, this.tableName)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink(stream: DataStream[Student]): Unit = {
    // 方式一、DataStream中的数据类型为HBaseBaseBean的子类
    // stream.hbasePutDS(this.tableName)
    //this.flink.hbasePutDS(stream, this.tableName)

    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    //stream.hbasePutDS2(this.tableName)(value => value)
    // 或者
    // this.flink.hbasePutDS2(stream, this.tableName)(value => value)
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink2(stream: DataStream[Student]): Unit = {
    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    //stream.hbasePutDS2(this.tableName)(value => value)
    // 或者
    //this.flink.hbasePutDS2(stream, this.tableName)(value => value)
  }

  /**
   * hbase的基本操作
   */
  def testHBase: Unit = {
    // get操作
    val getList = ListBuffer(HBaseConnector.buildGet("12"))
    val student = HBaseConnector.get(this.tableName, classOf[Student], getList, 1)
    if (student != null) println(JSON.toJSONString(student, SerializerFeature.NotWriteDefaultValue))
    // scan操作
    val studentList = HBaseConnector.scan(this.tableName, classOf[Student], HBaseConnector.buildScan("0", "9"), 1)
    if (studentList != null) println(JSON.toJSONString(studentList, SerializerFeature.NotWriteDefaultValue))
    // delete操作
    HBaseConnector.deleteRows(this.tableName, Seq("12"))
  }

  override def process: Unit = {
    require(this.args != null && this.args.length > 0, "请传递main方法参数")
    val stream = this.fire.createKafkaDirectStream().filter(JSONUtils.checkJson(_)).map(json => JSON.parseObject(json, classOf[Student]))

    this.args(0) match {
      case "testTableHBaseSink" => this.testTableHBaseSink(stream)
      case "testTableHBaseSink2" => this.testTableHBaseSink2(stream)
      case "testStreamHBaseSink" => this.testStreamHBaseSink(stream)
      case "testStreamHBaseSink2" => this.testStreamHBaseSink2(stream)
      case "testHBase" => this.testHBase
      case _ => throw new IllegalArgumentException("未匹配到任何方法名称，请检查！")
    }

    this.fire.start()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
