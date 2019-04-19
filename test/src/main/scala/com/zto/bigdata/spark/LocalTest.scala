package com.zto.bigdata.spark

import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.ext.BaseSparkCore
import com.zto.bigdata.spark.common.util.DateFormatUtils
import org.apache.spark.sql.SaveMode

object LocalTest extends BaseSparkCore {

  val table = "student"


  def main(args: Array[String]): Unit = {
    this.init()

    val studentRDD = this.spark.sparkContext.parallelize(1 to 18).map(i => new Student(i.toLong, "admin" + i, i, new java.math.BigDecimal(i), true, DateFormatUtils.formatCurrentDateTime()))
    val studentDF = this.spark.createDataFrame(studentRDD, classOf[Student])
    studentDF.write.format("jdbc").option("dbtable", "presto_monitor.student").option("url", "jdbc:mysql://localhost:3306/presto_monitor")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Merge).save()

    this.spark.stop()
  }

}
