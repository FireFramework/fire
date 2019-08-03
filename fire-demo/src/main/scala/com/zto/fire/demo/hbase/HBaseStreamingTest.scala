package com.zto.fire.demo.hbase

import com.zto.fire.common.db.HBaseOper
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.Student2

/**
  * 通过hbase相关api，将数据实时写入到hbase中
  * @author ChengLong 2019-5-26 13:21:59
  */
object HBaseStreamingTest extends BaseSparkStreaming {
  private val tableName = "fire_test"

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()

    dstream.repartition(10).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        this.mark
        HBaseOper.insert(this.tableName, Student2.buildStudentList())
        val student = HBaseOper.get(this.tableName, "1", classOf[Student2])
        this.log(student.toString)
      })
    })

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(30, false)
  }
}
