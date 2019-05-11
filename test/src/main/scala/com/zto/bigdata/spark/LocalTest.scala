package com.zto.bigdata.spark

import com.zto.bigdata.spark.common.ext.BaseSparkCore

object LocalTest extends BaseSparkCore {

  val table = "student"

  def main(args: Array[String]): Unit = {
    this.init()

    this.spark.sql("use tmp")
    this.spark.sql("show tables").show(100, false)

    /*val studentRDD = this.spark.sparkContext.parallelize(1 to 18).map(i => new Student(i.toLong, "admin" + i, i, new java.math.BigDecimal(i), true, DateFormatUtils.formatCurrentDateTime()))
    val studentDF = this.spark.createDataFrame(studentRDD, classOf[Student])
    studentDF.saveToJDBC("presto_monitor", "student", SaveMode.Merge)*/

    this.spark.stop()
  }

}
