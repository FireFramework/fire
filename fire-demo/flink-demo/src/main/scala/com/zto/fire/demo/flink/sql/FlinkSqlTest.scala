package com.zto.fire.demo.flink.sql

import com.alibaba.fastjson.JSON
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.api.scala._

/**
 * flink sql connector
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-10-21 11:08
 */
object FlinkSqlTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val stream = this.env.createDirectStream().filter(JSONUtils.checkJson(_)).map(json => JSON.parseObject(json, classOf[Student]))
    stream.createOrReplaceTempView("student")
    this.flink.sqlUpdate(
      """
        |CREATE TABLE spark_test2(
        |name string,
        |age int,
        |createTime string,
        |sex boolean
        |) WITH (
        |'connector.type' = 'jdbc',
        |'connector.url' = 'jdbc:mysql://10.9.15.251:3306/datax?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true',
        |'connector.table' = 'spark_test',
        |'connector.driver' = 'com.mysql.jdbc.Driver',
        |'connector.username' = 'ztm2',
        |'connector.password' = 'ztm2Test$!',
        |'connector.write.flush.max-rows' = '2',
        |'connector.write.flush.interval' = '30s',
        |'connector.write.max-retries' = '3'
        |)
        |""".stripMargin)

    this.flink.sqlUpdate(
      """
        | insert into spark_test2(name, age, createTime, sex)
        | select name, age, createTime, sex
        | from student
        | group by name, age, createTime, sex
        |""".stripMargin)

    this.env.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
