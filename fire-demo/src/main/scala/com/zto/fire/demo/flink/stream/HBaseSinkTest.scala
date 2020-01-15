package com.zto.fire.demo.flink.stream

import com.zto.fire.common.bean.{HBaseBaseBean, MultiVersionsBean}
import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.PropUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.sink.HBaseOperSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.JavaConversions

/**
 * 自定义HBaseSink
 *
 * @author ChengLong 2020年1月15日 16:05:56
 * @since 0.4.1
 */
object HBaseSinkTest extends BaseFlinkStreaming {

  override def process: Unit = {
    PropUtils.toFlinkConfMap.foreach(t => println(t._1 + " -> " + t._2))
    val dataStream = this.ssc.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    dataStream.hbaseOperPut("fire_test_1")
    this.ssc.execute("hbase sink test")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}

