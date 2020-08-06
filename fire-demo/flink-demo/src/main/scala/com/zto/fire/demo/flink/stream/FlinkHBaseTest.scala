package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
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
    val table = this.flink.sql("select id, name, age from student group by id, name, age")
    // 方式一、自动将row转为对应的JavaBean
    // table.hbaseOperPutTable(this.tableName, classOf[Student])
    this.flink.hbaseOperPutTable(table, this.tableName, classOf[Student], multiVersion = true)

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
    // stream.hbaseOperPutDS(this.tableName)
    // this.flink.hbaseOperPutDS(stream, this.tableName)

    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    //stream.hbaseOperPutDS2(this.tableName)(value => value)
    // 或者
    // this.flink.hbaseOperPutDS2(stream, this.tableName)(value => value)
  }

  /**
   * hbase的基本操作
   */
  def testHBase: Unit = {
    // get操作
    val student = HBaseOper.get(this.tableName, HBaseOper.buildGet("12"), classOf[Student])
    if (student != null) println(JSON.toJSONString(student, SerializerFeature.NotWriteDefaultValue))
    // scan操作
    val studentList = HBaseOper.scan(this.tableName, HBaseOper.buildScan("0", "9"), classOf[Student])
    if (studentList != null) println(JSON.toJSONString(studentList, SerializerFeature.NotWriteDefaultValue))
    // delete操作
    HBaseOper.deleteRow(this.tableName, "12")
  }

  override def process: Unit = {
    val stream = this.ssc.createDirectStream().filter(JSONUtils.checkJson(_)).map(json => JSON.parseObject(json, classOf[Student]))

    this.testTableHBaseSink(stream)
    // this.testStreamHBaseSink(stream)
    this.testHBase

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
