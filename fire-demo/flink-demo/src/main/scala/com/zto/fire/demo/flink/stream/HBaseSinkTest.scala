package com.zto.fire.demo.flink.stream

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.bean.{HBaseBaseBean, MultiVersionsBean}
import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.PropUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.sink.{HBaseOperSink, HBaseOperSinkBatch}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

/**
 * 自定义HBaseSink
 *
 * @author ChengLong 2020年1月15日 16:05:56
 * @since 0.4.1
 */
object HBaseSinkTest extends BaseFlinkStreaming {

  @Scheduled(fixedInterval = 5000)
  def job: Unit = {
    println("====定时执行====")
  }

  override def process: Unit = {
    PropUtils.toFlinkConfMap.foreach(t => println(t._1 + " -> " + t._2))
    val dataStream = this.ssc.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    dataStream.hbaseOperPut("fire_test_1")
    this.ssc.execute("hbase sink test")
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}

