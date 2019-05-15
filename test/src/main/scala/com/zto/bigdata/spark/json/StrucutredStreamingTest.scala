package com.zto.bigdata.spark.json

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.ext.BaseStructuredStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._

/**
  * 本示例用于演示structured streaming如何将kafka中的json数据解析为指定的DataFrame
  *
  * @author ChengLong 2019-4-26 16:58:06
  */
object StrucutredStreamingTest extends BaseStructuredStreaming {

  def main(args: Array[String]): Unit = {
    this.init()

    // 以子线程方式运行
    this.runAsThread(write2Carbondata)
  }

  /**
    * 数据写入到carbondata中
    */
  def write2Carbondata: Unit = {
    // kafka相关配置信息请见：spark.kafka.topics | spark.kafka.brokers.url | spark.kafka.starting.offsets | spark.kafka.failOnDataLoss
    // 消费指定topic数据，并将json数据解析为指定的javabean，最后一个参数为true表示解析before字段，默认只解析after字段
    // classOf[Senda]表示json的schema信息与Senda这个javabean对应，需创建一个javabean与json中需要解析的字段相对应
    val result = this.spark.loadKafkaParseJson(classOf[Senda])
    result.printSchema()
    // 查询after中的字段
    result.select("bill_code").printSchema()
    // 将信息打印到控制台
  }

}
