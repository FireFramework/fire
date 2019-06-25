package com.zto.fire.demo

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.demo.bean.Student

object LocalTest extends BaseSparkCore {


  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    val studentDF = this.spark.createDataFrame(Student.newStudentList(), classOf[Student])
    studentDF.schema.printTreeString()
  }

  def main(args: Array[String]): Unit = {
    this.init()
    this.spark.stop()
  }
}
