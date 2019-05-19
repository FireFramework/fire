package com.zto.bigdata.spark.tidb

import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.core.BaseSparkCore
import com.zto.bigdata.spark.common.util.SparkUtils
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import com.zto.bigdata.spark.common.ext.SparkExt._

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
    df.hbaseOperInsertDF(this.tableName, classOf[Student])

    val rowKeyRDD = this.sc.parallelize(Seq("1001", "2010", "20012"))
    // 读取hbase数据，并转为Student类型的DataFrame
    val studentDF = rowKeyRDD.hbaseBulkGet(this.tableName, classOf[Student])

    this.spark.stop()
  }
}
