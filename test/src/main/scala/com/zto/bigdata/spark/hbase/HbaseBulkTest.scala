package com.zto.bigdata.spark.hbase

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.ext.BaseSparkCore
import com.zto.bigdata.spark.common.ext.SparkExt._

/**
  * 在spark中使用 bulk 的方式读写hbase表（大量数据的情况下性能更高）
  * @author ChengLong 2019-5-9 09:38:06
  */
object HbaseBulkTest extends BaseSparkCore {
  private val tableName = "zto_test_senda"

  def main(args: Array[String]): Unit = {
    this.init()
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    // 将rdd的数据写入到hbase中，rdd类型必须为HBaseBaseBean的子类
    val rdd = this.sc.parallelize(Student.buildStudentList().toScalaList)
    rdd.hbaseBulkPut(this.tableName)

    // 使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.sc.parallelize(Seq(1.toString, 2.toString))
    val studentRDD = rowKeyRdd.hbaseBulkGet(this.tableName, classOf[Student])

    studentRDD.foreach(stu => {
      println("--> " + stu)
      println(JSON.toJSONString(stu, SerializerFeature.WriteNullListAsEmpty))
    })
    this.spark.stop()
  }
}
