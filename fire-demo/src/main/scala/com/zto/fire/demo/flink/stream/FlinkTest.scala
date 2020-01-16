package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.bean.FlinkTableSchema
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.util.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object FlinkTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream().map(json => JSON.parseObject(json, classOf[Student]))
    dstream.createOrReplaceTempView("student")
    val table = this.flink.sql("select * from student")
    val tableSchema = new FlinkTableSchema(table.getSchema)

    this.flink.toRetractStream[Row](table).addSink(t => {
      val student: Student = FlinkUtils.flinkRowToBean(tableSchema, t._2, classOf[Student])
      println("-------->" + student.toString)
    })

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
