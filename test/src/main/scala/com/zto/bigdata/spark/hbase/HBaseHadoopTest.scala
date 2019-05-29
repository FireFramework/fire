package com.zto.bigdata.spark.hbase

import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.core.BaseSparkCore
import org.apache.spark.sql.{Encoders, Row}

import scala.collection.JavaConversions

/**
  * 本示例演示Spark提供的hbase api封装后的使用
  * 注：使用Spark写hbase的方式适用于海量数据离线写
  *
  * @author ChengLong 2019-5-9 09:37:25
  */
object HBaseHadoopTest extends BaseSparkCore {
  private val tableName1 = "zto_test_senda"
  private val tableName2 = "zto_test_senda2"
  private val tableName3 = "zto_test_senda3"
  private val tableName4 = "zto_test_senda4"

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
    this.spark.sql("select age,createTime,id,length,name,sex from student").hbaseHadoopPutDFRow(this.tableName4, buildRowKey, false)
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为RDD
    */
  def testHBaseHadoopScanRDD: Unit = {
    val studentRDD = this.spark.hbaseHadoopScanRDD2(this.tableName2, "1", "6", classOf[Student])
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
    * 使用Spark的方式scan海量数据，并将结果集映射为Dataset
    */
  def testHBaseHadoopScanDS: Unit = {
    val studentDS = this.spark.hbaseHadoopScanDS2(this.tableName4, "1", "6", classOf[Student])
    studentDS.show(100, false)
  }

  /**
    * 使用bulk方式批量删除指定的rowKey对应的数据
    */
  def testHBaseBulkDeleteRDD: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.spark.parallelize(Seq(1.toString, 2.toString, 5.toString, 8.toString))
    // 根据rowKey删除
    rowKeyRdd.hbaseBulkDeleteRDD(null)

    // 方式二：使用this.spark.hbaseBulkDeleteRDD
    // this.spark.hbaseBulkDeleteRDD(this.tableName1, rowKeyRdd)
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    /*this.testHbaseHadoopPutRDD
    this.testHbaseHadoopPutDF
    this.testHbaseHadoopPutDS*/
    // this.testHbaseHadoopPutDFRow

    // this.testHBaseHadoopScanRDD
    // this.testHBaseHadoopScanDF
    // this.testHBaseHadoopScanDS
    this.testHBaseBulkDeleteRDD
  }

  def main(args: Array[String]): Unit = {
    this.init()
    this.spark.stop()
  }
}
