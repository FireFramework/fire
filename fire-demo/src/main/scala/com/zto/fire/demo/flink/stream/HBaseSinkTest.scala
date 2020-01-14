package com.zto.fire.demo.flink.stream

import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.PropUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.JavaConversions

object HBaseSinkTest extends BaseFlinkStreaming {

  override def process: Unit = {
    PropUtils.toFlinkConfMap.foreach(t => println(t._1 + " -> " + t._2))
    val dataset = this.ssc.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    dataset.addSink(new HBaseSink)
    this.ssc.execute("udf test")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}

class HBaseSink extends RichSinkFunction[Student] {
  override def open(parameters: Configuration): Unit = {
    println("======open()======")
  }

  override def close(): Unit = {
    println("======close()======")
  }

  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    HBaseOper.insert("fire_test_1", value)
  }
}