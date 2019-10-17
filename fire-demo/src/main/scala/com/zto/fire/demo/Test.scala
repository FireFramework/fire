package com.zto.fire.demo

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.ScanSendModel

object Test extends BaseSparkStreaming {

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD(rdd => {
      val parseDF = rdd.kafkaJson2DF(classOf[ScanSendModel], isMySQL = false, fieldNameUpper = false)
      parseDF.show(2, false)
      // parseDF.select("after.*").show(2, false)
      /*rdd.kafkaJson2Table("test")
      this.spark.sql("select * from test").show(20, false)*/
      // this.spark.sql("select * from scan_send where pda_code='自动分拣'").show(10, false)*/
    })
    this.ssc.startAwaitTermination()
  }


  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
