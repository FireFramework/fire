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
  private val tableName3 = "fire_test_3"
  private val tableName5 = "fire_test_5"

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    HBaseConnector.truncateTable(this.tableName3)
    HBaseConnector.truncateTable(this.tableName5, keyNum = 2)

    dstream.repartition(3).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        HBaseConnector.insert(this.tableName3, Student.newStudentList())
        val student = HBaseConnector.get(this.tableName5, classOf[Student], Seq("1", "2"))
        student.foreach(t => logger.error("HBase1 Get结果：" + t))

        HBaseConnector.insert(this.tableName5, Student.newStudentList())
        val student2 = HBaseConnector.get(this.tableName3, classOf[Student], Seq("2", "3"), keyNum = 2)
        student2.foreach(t => logger.error("HBase2 Get结果：" + t))
      })
    })

    this.fire.start()
  }

  def main(args: Array[String]): Unit = {
    this.init(30, false)
  }
}
