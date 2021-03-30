package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.common.util.{DateFormatUtils, PropUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.spark.jdbc.JdbcTest.tableName
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.{BaseSparkCore, BaseSparkStreaming}


/**
 * 基于Fire进行Spark Streaming开发
 */
object Test extends BaseSparkCore {

  override def process: Unit = {
    val ds = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    ds.createOrReplaceTempView("test")
    this.fire.sql("select * from test").print()
    this.fire.sql("select * from dim.baseorganize_addzero limit 10").show()
    this.fire.stop
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
