package com.zto.fire.examples.spark.tidb

import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkCore

import scala.collection.JavaConverters._
import com.zto.fire.spark.ext.SparkExt._

/**
  * 测试tidb写入
  */
object TIWriter extends BaseSparkCore {
  val tableName = "t_student"

  def main(args: Array[String]): Unit = {
    this.init()
    val studentRDD = this.sc.parallelize(Student.newStudentList().asScala)
    val df = this.spark.createDataFrame(studentRDD, classOf[Student])
    // Student类型的DataFrame数据写入hbase
    df.hbaseOperPutDF(this.tableName, classOf[Student])
    this.spark.stop()
  }
}
