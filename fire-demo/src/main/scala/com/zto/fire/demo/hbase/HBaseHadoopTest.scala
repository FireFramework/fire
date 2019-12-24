package com.zto.fire.demo.hbase

import com.zto.fire.common.db.HBaseOper
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.demo.bean.Student
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{CompareFilter, RegexStringComparator, RowFilter}
import org.apache.spark.sql.{Encoders, Row}

import scala.collection.JavaConversions

/**
  * 本示例演示Spark提供的hbase api封装后的使用
  * 注：使用Spark写hbase的方式适用于海量数据离线写
  *
  * @author ChengLong 2019-5-9 09:37:25
  */
object HBaseHadoopTest extends BaseSparkCore {
  private val tableName1 = "fire_test_1"
  private val tableName2 = "fire_test_2"
  private val tableName3 = "fire_test_3"
  private val tableName4 = "fire_test_4"

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将rdd数据保存到hbase中
    */
  def testHbaseHadoopPutRDD: Unit = {
    val studentRDD = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    this.spark.hbaseHadoopPutRDD(this.tableName1, studentRDD)
    // 方式二：直接基于rdd进行方法调用
    // studentRDD.hbaseHadoopPutRDD(this.tableName1)
  }

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将DataFrame数据保存到hbase中
    */
  def testHbaseHadoopPutDF: Unit = {
    val studentRDD = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    val studentDF = this.spark.createDataFrame(studentRDD, classOf[Student])
    // 由于DataFrame相较于Dataset和RDD是弱类型的数据集合，所以需要传递具体的类型classOf[Type]
    this.spark.hbaseHadoopPutDF(this.tableName2, studentDF, classOf[Student])
    // 方式二：基于DataFrame进行方法调用
    // studentDF.hbaseHadoopPutDF(this.tableName2, classOf[Student])
  }

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将Dataset数据保存到hbase中
    */
  def testHbaseHadoopPutDS: Unit = {
    val studentDS = this.spark.createDataset(JavaConversions.asScalaBuffer(Student.buildStudentList()))(Encoders.bean(classOf[Student]))
    this.spark.hbaseHadoopPutDS(this.tableName3, studentDS)
    // 方式二：基于DataFrame进行方法调用
    // studentDS.hbaseHadoopPutDS(this.tableName3)
  }

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将不是HBaseBaseBean结构对应的DataFrame保存到hbase中
    * 注：此方法与hbaseHadoopPutDF不同之处在于，它不强制要求该DataFrame一定要与HBaseBaseBean的子类对应
    * 但需要指定rowKey的构建规则，相对与hbaseHadoopPutDF来说，少了中间的两次转换，性能会更高
    */
  def testHbaseHadoopPutDFRow: Unit = {
    /**
      * 构建main_order rowkey
      */
    val buildRowKey = (row: Row) => {
      // 将id字段作为rowKey
      row.getAs("id").toString
    }

    val studentRDD = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    this.spark.createDataFrame(studentRDD, classOf[Student]).createOrReplaceTempView("student")
    // 指定rowKey构建的函数
    this.spark.sql("select age,createTime,id,length,name,sex from student").hbaseHadoopPutDFRow(this.tableName3, buildRowKey, false)
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为RDD
    */
  def testHBaseHadoopScanRDD: Unit = {
    val daysFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("20190613|20190614|20190615|20190616"))
    val studentRDD = this.spark.hbaseHadoopScanRDD(this.tableName2, new Scan().setFilter(daysFilter), classOf[Student])
    studentRDD.printEachPartition
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为DataFrame
    */
  def testHBaseHadoopScanDF: Unit = {
    val studentDF = this.spark.hbaseHadoopScanDF2(this.tableName3, "1", "6", classOf[Student])
    studentDF.show(100, false)
  }

  /**
   * 使用Spark的方式scan海量数据，并将结果集注册成临时表
   */
  def testHBaseHadoopScanTable: Unit = {
    println("=============scan后将结果集注册成临时表================")
    this.spark.hbaseHadoopScanTable(this.tableName3, HBaseOper.buildScan("1", "3"), classOf[Student])
    this.spark.sql(s"select * from ${this.tableName3}").show(100, false)
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集注册成临时表
    */
  def testHBaseHadoopScanTable2: Unit = {
    println("=============scan后将结果集注册成临时表2================")
    val studentDF = this.spark.hbaseHadoopScanTable2(this.tableName3, "4", "6", classOf[Student])
    this.spark.sql(s"select * from ${this.tableName3}").show(100, false)
    studentDF.show()
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为Dataset
    */
  def testHBaseHadoopScanDS: Unit = {
    val studentDS = this.spark.hbaseHadoopScanDS2(this.tableName3, "1", "6", classOf[Student])
    studentDS.show(100, false)
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    /*this.testHbaseHadoopPutRDD
    this.testHbaseHadoopPutDF
    this.testHbaseHadoopPutDS*/
    this.testHbaseHadoopPutDFRow

    // this.testHBaseHadoopScanRDD
    // this.testHBaseHadoopScanDF
    // this.testHBaseHadoopScanDS
    this.testHBaseHadoopScanTable
    this.testHBaseHadoopScanTable2
  }

  def main(args: Array[String]): Unit = {
    this.init()
    this.spark.stop()
  }
}
