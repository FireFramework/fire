package com.zto.fire.examples.spark.structured

import com.zto.fire._
import com.zto.fire.spark.BaseStructuredStreaming

/**
 * 使用fire进行structured streaming开发的demo
 *
 * @author ChengLong 2019年12月23日 22:16:59
 */
object StructuredStreamingTest extends BaseStructuredStreaming {

  /**
   * structured streaming处理逻辑
   */
  override def process: Unit = {
    // 接入kafka消息，并将消息解析为DataFrame，同时注册临时表，表名默认为kafka，也可传参手动指定表名
    val kafkaDataset = this.spark.loadKafkaParseJson()
    // 进行sql查询，支持嵌套的json，并且支持大小写的json
    this.spark.sql("select table, after.bill_code, after.scan_site from kafka").print()
    // 使用api的方式进行查询操作
    kafkaDataset.select("after.PDA_CODE", "after.bill_code").print(numRows = 1, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}