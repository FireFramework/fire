package com.zto.fire.examples.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.ext.FlinkExt._
import org.apache.flink.api.scala._

object FlinkRetractStreamTest extends BaseFlinkStreaming {

  val tableName = "spark_test"

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream().map(json => JSON.parseObject(json, classOf[Student])).shuffle
    dstream.createOrReplaceTempView("student")
    val table = this.flink.sql("select name, age, createTime, length, sex from student group by name, age, createTime, length, sex")

    val fields = "name, age, createTime, length, sex"
    val sql = s"INSERT INTO $tableName ($fields) VALUES (?, ?, ?, ?, ?)"
    // 方式一、table中的列顺序和类型需与jdbc sql中的占位符顺序保持一致
    // table.jdbcBatchUpdate(sql, keyNum = 1)
    // 方式二、自定义row取数规则，该种方式较灵活，可定义取不同的列，顺序仍需与sql占位符保持一致
    table.jdbcBatchUpdate2(sql, batch = 10, flushInterval = 10000)(row => Seq(row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4)))

    // toRetractStream支持状态更新、删除操作，比例sql中含有group by 等聚合操作，后进来的记录会导致已有的聚合结果不正确
    // 使用toRetractStream后会将之前的旧的聚合结果重新发送一次，并且tuple中的flag标记为false，然后再发送一条正确的结果
    // 类似于structured streaming中自动维护结果表，并进行update操作
    // this.flink.toRetractStream[Row](table).print()

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
