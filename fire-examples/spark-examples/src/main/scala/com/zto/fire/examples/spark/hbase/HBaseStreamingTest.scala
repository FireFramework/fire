package com.zto.fire.examples.spark.hbase

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.BaseSparkStreaming

/**
  * 通过hbase相关api，将数据实时写入到hbase中
  * @author ChengLong 2019-5-26 13:21:59
  */
object HBaseStreamingTest extends BaseSparkStreaming {
  private val tableName = "fire_test_5"

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()

    dstream.repartition(5).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        HBaseConnector.insert(this.tableName, Student.newStudentList())
        val student = HBaseConnector.get(this.tableName, classOf[Student], Seq("1"))
      })
    })

    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    this.init(30, false)
  }
}
