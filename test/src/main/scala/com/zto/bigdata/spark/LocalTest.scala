package com.zto.bigdata.spark

import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.ext.BaseSparkCore
import org.apache.spark.sql.SaveMode
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.DateFormatUtils

object LocalTest extends BaseSparkCore {

  val table = "student"

  def main(args: Array[String]): Unit = {
    this.init()

    /*val studentRDD = this.spark.sparkContext.parallelize(1 to 18).map(i => new Student(i.toLong, "admin" + i, i, new java.math.BigDecimal(i), true, DateFormatUtils.formatCurrentDateTime()))
    val studentDF = this.spark.createDataFrame(studentRDD, classOf[Student])
    studentDF.saveToJDBC("presto_monitor", "student", SaveMode.Merge)*/

    this.spark.stop()
  }

}
