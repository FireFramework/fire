package com.zto.fire.demo.structured

import com.zto.fire.core.BaseStructuredStreaming
import com.zto.fire.core.ext.SparkExt._

object KafkaStructuredTest extends BaseStructuredStreaming {

  /**
   * Spark处理逻辑
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val kafkaDataset = this.spark.loadKafkaParseJson()
    kafkaDataset.select("data.after.PDA_CODE", "data.after.bill_code").print(numRows = 1, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}