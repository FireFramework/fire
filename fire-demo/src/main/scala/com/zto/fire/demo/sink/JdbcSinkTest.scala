package com.zto.fire.demo.sink

import com.zto.fire.core.BaseStructuredStreaming
import com.zto.fire.core.ext.SparkExt._

/**
 * 结构化流测试
 */
object JdbcSinkTest extends BaseStructuredStreaming {

  override def process: Unit = {
    // 接入kafka并解析json，支持大小写，默认表名为data
    val kafkaDataset = this.spark.loadKafkaParseJson()

    // 将流数据持续写入到关系型数据库中
    kafkaDataset.select("data.age", "data.name").jdbcBatchUpdate("insert into spark_test(age, name) values(?,?)")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}