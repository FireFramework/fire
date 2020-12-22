package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.alibaba.fastjson.JSON
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * 自定义sink的实现
 */
object FlinkSinkTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream().map(json => JSON.parseObject(json, classOf[Student]))
    dstream.map(t => t.getName).addSink(new MySink).setParallelism(1)

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}

class MySink extends RichSinkFunction[String] {

  /**
   * open方法中可以创建数据库连接等初始化操作
   * 注：若setParallelism(10)则会执行10次open方法
   */
  override def open(parameters: Configuration): Unit = {
    println("=========执行open方法========")
  }

  /**
   * close方法用于释放资源，如数据库连接等
   */
  override def close(): Unit = {
    println("=========执行close方法========")
  }

  override def invoke(value: String, context: SinkFunction.Context): Unit = {
    println("---> " + value)
  }
}
