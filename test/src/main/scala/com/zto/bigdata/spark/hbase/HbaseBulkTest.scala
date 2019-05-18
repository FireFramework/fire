package com.zto.bigdata.spark.hbase

import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.core.BaseSparkCore
import com.zto.bigdata.spark.common.ext.SparkExt._

import scala.collection.JavaConversions

/**
  * 本示例用于演示spark中使用bulk api完成HBase的读写
  * bulk api相较于java api，在速度上会更快，但目前暂不支持多版本读写
  *
  * @author ChengLong 2019-5-18 09:20:52
  */
object HbaseBulkTest extends BaseSparkCore {
  private val tableName = "zto_test_senda"

  /**
    * 使用bulk的方式写hbase
    */
  def testBulkPut: Unit = {
    // 方式一：将rdd的数据写入到hbase中，rdd类型必须为HBaseBaseBean的子类
    val rdd = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    rdd.hbaseBulkPut(this.tableName)

    // 方式二：使用this.spark.hbaseBulkPut将rdd中的数据写入到hbase
    // this.spark.hbaseBulkPut(this.tableName, rdd)

    // 第二个参数指定false表示不插入为null的字段到hbase中
    // rdd.hbaseBulkPut(this.tableName, false)
    // 第三个参数为true表示以多版本json格式写入
    // rdd.hbaseBulkPut(this.tableName, false, true)
  }

  /**
    * 使用bulk方式根据rowKey获取数据
    */
  def testBulkGet: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.spark.parallelize(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString))
    val studentRDD = rowKeyRdd.hbaseBulkGet(this.tableName, classOf[Student])
    studentRDD.foreach(println)

    // 方式二：使用this.spark.hbaseBulkGet
    // val studentRDD2 = this.spark.hbaseBulkGet(this.tableName, rowKeyRdd, classOf[Student])
  }

  /**
    * 使用bulk方式批量删除指定的rowKey对应的数据
    */
  def testBulkDelete: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.spark.parallelize(Seq(1.toString, 2.toString, 5.toString))
    // 根据rowKey删除
    rowKeyRdd.hbaseBulkDelete(this.tableName)

    // 方式二：使用this.spark.hbaseBulkDelete
    // this.spark.hbaseBulkDelete(this.tableName, rowKeyRdd)
  }

  /**
    * 使用bulk方式进行scan
    */
  def testBulkScan: Unit = {
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为RDD[Student]
    val scanRDD = this.spark.hbaseBulkScan(this.tableName, "1", "6", classOf[Student])
    scanRDD.foreach(println)

    // 将scan后得到的数据直接转为DataFrame类型
    val df = this.spark.hbaseBulkScanDF(this.tableName, "1", "6", classOf[Student])
    df.printSchema()
    df.show(100, false)
  }


  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    // this.testBulkPut
    this.testBulkDelete
    this.testBulkGet
    // this.testBulkScan
  }

  def main(args: Array[String]): Unit = {
    this.init()

    this.spark.stop()
  }

}
