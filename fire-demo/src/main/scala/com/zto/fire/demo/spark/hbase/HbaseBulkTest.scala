package com.zto.fire.demo.spark.hbase

import com.zto.fire.common.db.HBaseOper
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.demo.bean.Student
import org.apache.spark.sql.{Encoders, Row}

import scala.collection.JavaConversions

/**
  * 本示例用于演示spark中使用bulk api完成HBase的读写
  * 注：bulk api相较于java api，在速度上会更快，但目前暂不支持多版本读写
  *
  * @author ChengLong 2019-5-18 09:20:52
  */
object HBaseBulkTest extends BaseSparkCore {
  private val tableName1 = "fire_test_1"
  private val tableName2 = "fire_test_2"
  private val tableName3 = "fire_test_3"
  private val tableName4 = "fire_test_4"

  /**
    * 使用id作为rowKey
    */
  val buildStudentRowKey = (row: Row) => {
    row.getAs("id").toString
  }

  /**
    * 使用bulk的方式将rdd写入到hbase
    */
  def testHbaseBulkPutRDD: Unit = {
    // 方式一：将rdd的数据写入到hbase中，rdd类型必须为HBaseBaseBean的子类
    val rdd = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    rdd.hbaseBulkPutRDD(this.tableName1)
    // 方式二：使用this.spark.hbaseBulkPut将rdd中的数据写入到hbase
    this.spark.hbaseBulkPutRDD(this.tableName1, rdd)

    // 第二个参数指定false表示不插入为null的字段到hbase中
    // rdd.hbaseBulkPutRDD(this.tableName2, insertEmpty = false)
    // 第三个参数为true表示以多版本json格式写入
    // rdd.hbaseBulkPutRDD(this.tableName3, false, true)
  }

  /**
    * 使用bulk的方式将DataFrame写入到hbase
    */
  def testHbaseBulkPutDF: Unit = {
    // 方式一：将DataFrame的数据写入到hbase中
    val rdd = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    val studentDF = this.spark.createDataFrame(rdd, classOf[Student])
    // insertEmpty=false表示为空的字段不插入
    studentDF.hbaseBulkPutDF(this.tableName2, classOf[Student], false)
    // 方式二：
    // this.spark.hbaseBulkPutDF(this.tableName2, studentDF, classOf[Student])
  }

  /**
    * 使用bulk的方式将Dataset写入到hbase
    */
  def testHbaseBulkPutDS: Unit = {
    // 方式一：将DataFrame的数据写入到hbase中
    val rdd = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    val studentDataset = this.spark.createDataset(rdd)(Encoders.bean(classOf[Student]))
    // multiVersion=true表示以多版本形式插入
    studentDataset.hbaseBulkPutDS(this.tableName2)
    // 方式二：
    // this.spark.hbaseBulkPutDS(this.tableName3, studentDataset)
  }

  /**
    * 使用bulk方式根据rowKey集合获取数据，并将结果集以RDD形式返回
    */
  def testHBaseBulkGetSeq: Unit = {
    // 方式一：使用rowKey集合读取hbase中的数据
    val seq = Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString)
    val studentRDD = this.spark.hbaseBulkGetSeq(this.tableName1, seq, classOf[Student])
    studentRDD.foreach(println)
    // 方式二：使用this.spark.hbaseBulkGetRDD
    val studentRDD2 = this.spark.hbaseBulkGetSeq(this.tableName2, seq, classOf[Student])
    studentRDD2.foreach(println)
  }

  /**
    * 使用bulk方式根据rowKey获取数据，并将结果集以RDD形式返回
    */
  def testHBaseBulkGetRDD: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.spark.parallelize(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString))
    val studentRDD = rowKeyRdd.hbaseBulkGetRDD(this.tableName1, classOf[Student])
    studentRDD.foreach(println)
    // 方式二：使用this.spark.hbaseBulkGetRDD
    // val studentRDD2 = this.spark.hbaseBulkGetRDD(this.tableName2, rowKeyRdd, classOf[Student])
    // studentRDD2.foreach(println)
  }

  /**
    * 使用bulk方式根据rowKey获取数据，并将结果集以DataFrame形式返回
    */
  def testHBaseBulkGetDF: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.spark.parallelize(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString))
    val studentDF = rowKeyRdd.hbaseBulkGetDF(this.tableName1, classOf[Student])
    studentDF.show(100, false)
    // 方式二：使用this.spark.hbaseBulkGetDF
    val studentDF2 = this.spark.hbaseBulkGetDF(this.tableName2, rowKeyRdd, classOf[Student])
    studentDF2.show(100, false)
  }

  /**
    * 使用bulk方式根据rowKey获取数据，并将结果集以Dataset形式返回
    */
  def testHBaseBulkGetDS: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.spark.parallelize(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString))
    val studentDS = rowKeyRdd.hbaseBulkGetDS(this.tableName2, classOf[Student])
    studentDS.show(100, false)
    // 方式二：使用this.spark.hbaseBulkGetDF
    // val studentDS2 = this.spark.hbaseBulkGetDS(this.tableName2, rowKeyRdd, classOf[Student])
    // studentDS2.show(100, false)
  }

  /**
    * 使用bulk方式进行scan，并将结果集映射为RDD
    */
  def testHbaseBulkScanRDD: Unit = {
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为RDD[Student]
    val scanRDD = this.spark.hbaseBulkScanRDD(this.tableName2, "1", "6", classOf[Student])
    scanRDD.foreach(println)
  }

  /**
    * 使用bulk方式进行scan，并将结果集映射为DataFrame
    */
  def testHbaseBulkScanDF: Unit = {
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为DataFrame
    val scanDF = this.spark.hbaseBulkScanDF(this.tableName2, "1", "6", classOf[Student])
    scanDF.show(100, false)
  }

  /**
    * 使用bulk方式进行scan，并将结果集映射为Dataset
    */
  def testHbaseBulkScanDS: Unit = {
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为Dataset[Student]
    val scanDS = this.spark.hbaseBulkScanDS(this.tableName2, HBaseOper.buildScan("1", "6"), classOf[Student])
    scanDS.show(100, false)
  }

  /**
    * 使用bulk方式批量删除指定的rowKey对应的数据
    */
  def testHBaseBulkDeleteRDD: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.spark.parallelize(Seq(1.toString, 2.toString, 5.toString, 8.toString))
    // 根据rowKey删除
    rowKeyRdd.hbaseBulkDeleteRDD(this.tableName1)

    // 方式二：使用this.spark.hbaseBulkDeleteRDD
    // this.spark.hbaseBulkDeleteRDD(this.tableName1, rowKeyRdd)
  }

  /**
    * 使用bulk方式批量删除指定的rowKey对应的数据
    */
  def testHBaseBulkDeleteDS: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.spark.parallelize(Seq(1.toString, 2.toString, 5.toString, 8.toString))
    // 根据rowKey删除
    this.spark.createDataset(rowKeyRdd)(Encoders.STRING).hbaseBulkDeleteDS(this.tableName2)

    // 方式二：使用this.spark.hbaseBulkDeleteDS
    // this.spark.hbaseBulkDeleteDS(this.tableName1, rowKeyRdd)
  }


  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    // this.testHbaseBulkPutRDD
    // this.testHbaseBulkPutDF
    this.testHbaseBulkPutDS

    // this.testHBaseBulkGetRDD
    // this.testHBaseBulkGetDF
    this.testHBaseBulkGetDS
    // this.testHBaseBulkGetSeq

    // this.testHBaseBulkDeleteRDD
    this.testHBaseBulkDeleteDS

    /*this.testHbaseBulkScanRDD
    this.testHbaseBulkScanDF*/
    this.testHbaseBulkScanDS
  }

  def main(args: Array[String]): Unit = {
    this.init()
    spark.stop()
  }

}
