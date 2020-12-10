package com.zto.fire.demo.spark.tidb

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.Student

import scala.collection.JavaConverters._

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
