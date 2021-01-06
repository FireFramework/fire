package com.zto.fire.examples.spark.hbase

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkCore
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
  private val tableName3 = "fire_test_3"

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将rdd数据保存到hbase中
    */
  def testHbaseHadoopPutRDD: Unit = {
    val studentRDD = this.fire.createRDD(JavaConversions.asScalaBuffer(Student.buildStudentList()), 2)
    this.fire.hbaseHadoopPutRDD(this.tableName3, studentRDD)
    // 方式二：直接基于rdd进行方法调用
    // studentRDD.hbaseHadoopPutRDD(this.tableName1)
  }

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将DataFrame数据保存到hbase中
    */
  def testHbaseHadoopPutDF: Unit = {
    val studentRDD = this.fire.createRDD(JavaConversions.asScalaBuffer(Student.buildStudentList()), 2)
    val studentDF = this.fire.createDataFrame(studentRDD, classOf[Student])
    // 由于DataFrame相较于Dataset和RDD是弱类型的数据集合，所以需要传递具体的类型classOf[Type]
    this.fire.hbaseHadoopPutDF(this.tableName3, studentDF, classOf[Student])
    // 方式二：基于DataFrame进行方法调用
    // studentDF.hbaseHadoopPutDF(this.tableName3, classOf[Student])
  }

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将Dataset数据保存到hbase中
    */
  def testHbaseHadoopPutDS: Unit = {
    val studentDS = this.fire.createDataset(JavaConversions.asScalaBuffer(Student.buildStudentList()))(Encoders.bean(classOf[Student]))
    this.fire.hbaseHadoopPutDS(this.tableName3, studentDS)
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

    val studentRDD = this.fire.createRDD(JavaConversions.asScalaBuffer(Student.buildStudentList()), 2)
    this.fire.createDataFrame(studentRDD, classOf[Student]).createOrReplaceTempView("student")
    // 指定rowKey构建的函数
    this.fire.sql("select age,createTime,id,length,name,sex from student").hbaseHadoopPutDFRow(this.tableName3, buildRowKey)
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为RDD
    */
  def testHBaseHadoopScanRDD: Unit = {
    println("===========testHBaseHadoopScanRDD===========")
    val daysFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("20190613|20190614|20190615|20190616"))
    val studentRDD = this.fire.hbaseHadoopScanRDD(this.tableName3, new Scan().setFilter(daysFilter), classOf[Student])
    studentRDD.printEachPartition
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为DataFrame
    */
  def testHBaseHadoopScanDF: Unit = {
    println("===========testHBaseHadoopScanDF===========")
    val studentDF = this.fire.hbaseHadoopScanDF2(this.tableName3, classOf[Student], "1", "6")
    studentDF.show(100, false)
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为Dataset
    */
  def testHBaseHadoopScanDS: Unit = {
    println("===========testHBaseHadoopScanDS===========")
    val scan = new Scan()
    scan.setTimeRange(1575216000000L, 1575648000000L)
    val studentDS = this.fire.hbaseHadoopScanDS(this.tableName3, classOf[Student], scan)
    studentDS.show(100, false)
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    this.testHbaseHadoopPutRDD
    // this.testHbaseHadoopPutDF
    // this.testHbaseHadoopPutDS
    // this.testHbaseHadoopPutDFRow

    this.testHBaseHadoopScanRDD
    this.testHBaseHadoopScanDF
    this.testHBaseHadoopScanDS
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
