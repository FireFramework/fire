package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.demo.flink.stream.FlinkJdbcTest.value
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.sink.FlinkJdbcSink
import org.apache.flink.api.scala._

/**
 * flink jdbc sink
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-05-22 11:10
 */
object FlinkJdbcTest extends BaseFlinkStreaming {
  lazy val tableName = "spark_test"

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream().map(json => JSON.parseObject(json, classOf[Student]))

    val sql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    dstream.jdbcBatchUpdate(sql, 3, 30000) {
      // 将数据组装成jdbc的占位符
      value => Seq(value.getName, value.getAge, DateFormatUtils.formatCurrentDateTime(), value.getLength, value.getSex)
    }.setParallelism(1)

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
