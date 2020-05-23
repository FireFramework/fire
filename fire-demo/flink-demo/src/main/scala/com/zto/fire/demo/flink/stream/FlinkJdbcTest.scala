package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
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

    val fields = "name, age, createTime, length, sex"
    val sql = s"INSERT INTO $tableName ($fields) VALUES (?, ?, ?, ?, ?)"

    // 方式一、指定字段列表，内部根据反射，自动获取DataStream中的数据并填充到sql中的占位符
    // 此处fields有两层含义：1. sql中的字段顺序（对应表） 2. DataStream中的JavaBean字段数据（对应JavaBean）
    // 注：要保证DataStream中字段名称是JavaBean的名称，非表中字段名称 顺序要与占位符顺序一致，个数也要一致
    dstream.jdbcBatchUpdate(sql, fields).setParallelism(1)

    // 方式二、通过用户指定的匿名函数方式进行数据的组装，适用于上面方法无法反射获取值的情况，适用面更广
    dstream.jdbcBatchUpdate2(sql, 3, 30000) {
      // 在此处指定取数逻辑，定义如何将dstream中每列数据映射到sql中的占位符
      value => Seq(value.getName, value.getAge, DateFormatUtils.formatCurrentDateTime(), value.getLength, value.getSex)
    }.setParallelism(1)

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
