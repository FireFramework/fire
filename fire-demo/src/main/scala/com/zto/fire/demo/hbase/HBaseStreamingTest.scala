package com.zto.fire.demo.hbase

import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.demo.bean.MainOrder

/**
  * 通过hbase相关api，将数据实时写入到hbase中
  * @author ChengLong 2019-5-26 13:21:59
  */
object HBaseStreamingTest extends BaseSparkStreaming {
  private val tableName = "test_for_spark_common"

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    // 多个kafka源的写法
    // val dstream2 = this.ssc.createDirectStream(keyNum = 2)

    dstream.foreachRDD(rdd => {
      // 将kafka中的json一键解析成对应的JavaBean，或者使用：this.spark.kafkaJson2DF()
      val orderDF = rdd.kafkaJson2DF(classOf[MainOrder])
      // 第二个参数为true表示解析mysql或oracle 所有字段，包括before、after、op_type等等
      // rdd.kafkaJson2DF(classOf[MainOrder], true)
      orderDF.printSchema()

      // 方式一：将数据写入到hbase表中
      // orderDF.hbaseBulkPutDF(this.tableName, classOf[MainOrder])
      // 方式二：使用this.spark方式调用
      /*orderDF.show(1, false)
      orderDF.hbaseOperPutDF(this.tableName, classOf[MainOrder])*/
    })

    // 维护offset
    dstream.kafkaCommitOffsets

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(30, false)
  }
}
