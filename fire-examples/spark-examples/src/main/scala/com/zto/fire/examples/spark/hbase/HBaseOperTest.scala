package com.zto.fire.examples.spark.hbase

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkCore
import org.apache.hadoop.hbase.client.Get
import org.apache.spark.sql.Encoders

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

/**
  * 在spark中使用java 同步 api (HBaseOper) 的方式读写hbase表
  * 注：适用于少量数据的实时读写，更轻量
  *
  * @author ChengLong 2019-5-9 09:37:25
  */
object HBaseOperTest extends BaseSparkCore {
  private val tableName1 = "fire_test_1"

  /**
    * 使用HBaseOper插入一个集合，可以是list、set等集合
    * 但集合的类型必须为HBaseBaseBean的子类
    */
  def testHbaseOperPutList: Unit = {
    val studentList = Student.newStudentList()
    this.spark.hbaseOperPutList(this.tableName1, JavaConversions.asScalaBuffer(studentList))
  }

  /**
    * 使用HBaseOper插入一个rdd的数据
    * rdd的类型必须为HBaseBaseBean的子类
    */
  def testHbaseOperPutRDD: Unit = {
    val studentList = Student.buildStudentList()
    val studentRDD = this.spark.parallelize(JavaConversions.asScalaBuffer(studentList), 2)
    // 为空的字段不插入
    studentRDD.hbaseOperPutRDD(this.tableName1)
  }

  /**
    * 使用HBaseOper插入一个DataFrame的数据
    */
  def testHbaseOperPutDF: Unit = {
    val studentList = Student.buildStudentList()
    val studentDF = this.spark.createDataFrame(studentList, classOf[Student])
    // 每个批次插100条
    studentDF.hbaseOperPutDF(this.tableName1, classOf[Student])
  }

  /**
    * 使用HBaseOper插入一个Dataset的数据
    * dataset的类型必须为HBaseBaseBean的子类
    */
  def testHbaseOperPutDS: Unit = {
    val studentList = Student.buildStudentList()
    val studentDS = this.spark.createDataset(JavaConversions.asScalaBuffer(studentList))(Encoders.bean(classOf[Student]))
    // 以多版本形式插入
    studentDS.hbaseOperPutDS(this.tableName1, classOf[Student])
  }

  /**
    * 使用HBaseOper get数据，并将结果以list方式返回
    */
  def testHbaseOperGetList: Unit = {
    println("===========testHbaseOperGetList===========")
    val rowKeys = Seq("1", "2", "3", "5", "6")
    val studentList = this.spark.hbaseOperGetList2(this.tableName1, classOf[Student], rowKeys)
    studentList.foreach(println)

    val getList = ListBuffer[Get]()
    rowKeys.map(rowkey => (getList += new Get(rowkey.getBytes)))
    // 获取多版本形式存放的记录，并获取最新的两个版本就
    val studentList2 = this.spark.hbaseOperGetList(this.tableName1, classOf[Student], getList)
    studentList2.foreach(println)
  }

  /**
    * 使用HBaseOper get数据，并将结果以RDD方式返回
    */
  def testHbaseOperGetRDD: Unit = {
    println("===========testHbaseOperGetRDD===========")
    val getList = Seq("1", "2", "3", "5", "6")
    val getRDD = this.spark.parallelize(getList, 2)
    // 以多版本方式get，并将结果集封装到rdd中返回
    val studentRDD = this.spark.hbaseOperGetRDD(this.tableName1, classOf[Student], getRDD)
    studentRDD.printEachPartition
  }

  /**
    * 使用HBaseOper get数据，并将结果以DataFrame方式返回
    */
  def testHbaseOperGetDF: Unit = {
    println("===========testHbaseOperGetDF===========")
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.spark.parallelize(getList, 3)
    // get到的结果以dataframe形式返回
    val studentDF = this.spark.hbaseOperGetDF(this.tableName1, classOf[Student], getRDD)
    studentDF.show(100, false)
  }

  /**
    * 使用HBaseOper get数据，并将结果以Dataset方式返回
    */
  def testHbaseOperGetDS: Unit = {
    println("===========testHbaseOperGetDS===========")
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.spark.parallelize(getList, 2)
    // 指定在多版本获取时只取最新的两个版本
    val studentDS = this.spark.hbaseOperGetDS(this.tableName1, classOf[Student], getRDD)
    studentDS.show(100, false)
  }

  /**
    * 使用HBaseOper scan数据，并以list方式返回
    */
  def testHbaseOperScanList: Unit = {
    println("===========testHbaseOperScanList===========")
    val list = this.spark.hbaseOperScanList2(this.tableName1, classOf[Student], "1", "6")
    list.foreach(println)
  }

  /**
    * 使用HBaseOper scan数据，并以RDD方式返回
    */
  def testHbaseOperScanRDD: Unit = {
    println("===========testHbaseOperScanRDD===========")
    val rdd = this.spark.hbaseOperScanRDD2(this.tableName1, classOf[Student], "1", "6")
    rdd.repartition(3).printEachPartition
  }

  /**
    * 使用HBaseOper scan数据，并以DataFrame方式返回
    */
  def testHbaseOperScanDF: Unit = {
    println("===========testHbaseOperScanDF===========")
    val dataFrame = this.spark.hbaseOperScanDF2(this.tableName1, classOf[Student], "1", "6")
    dataFrame.repartition(3).show(100, false)
  }

  /**
    * 使用HBaseOper scan数据，并以DataFrame方式返回
    */
  def testHbaseOperScanDS: Unit = {
    println("===========testHbaseOperScanDF===========")
    val dataSet = this.spark.hbaseOperScanDS2(this.tableName1, classOf[Student], "1", "6")
    dataSet.show(100, false)
  }

  /**
    * 根据指定的rowKey list，批量删除指定的记录
    */
  def testHbaseOperDeleteList: Unit = {
    val rowKeyList = Seq(1.toString, 2.toString, 5.toString, 8.toString)
    this.spark.hbaseOperDeleteList(this.tableName1, rowKeyList)
  }

  /**
    * 根据指定的rowKey rdd，批量删除指定的记录
    */
  def testHbaseOperDeleteRDD: Unit = {
    val rowKeyList = Seq(1.toString, 2.toString, 3.toString, 4.toString, 5.toString, 6.toString, 7.toString, 8.toString, 9.toString, 10.toString)
    val rowKeyRDD = this.spark.parallelize(rowKeyList, 2)
    rowKeyRDD.hbaseOperDeleteRDD(this.tableName1)
  }

  /**
    * 根据指定的rowKey dataset，批量删除指定的记录
    */
  def testHbaseOperDeleteDS: Unit = {
    val rowKeyList = Seq(1.toString, 2.toString, 5.toString, 8.toString)
    val rowKeyDS = this.spark.createDataset(rowKeyList)(Encoders.STRING)
    rowKeyDS.hbaseOperDeleteDS(this.tableName1)
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    // 指定是否以多版本的形式读写
    val multiVersion = false
    this.testHbaseOperDeleteRDD
    // this.testHbaseOperDeleteRDD
    // this.testHbaseOperDeleteDS
    this.testHbaseOperPutRDD
    // this.testHbaseOperPutList
    // this.testHbaseOperPutRDD
    // this.testHbaseOperPutDF
    // this.testHbaseOperPutDS

    println("=========get========")
    this.testHbaseOperGetList
    this.testHbaseOperGetRDD
    this.testHbaseOperGetDF
    this.testHbaseOperGetDS

    println("=========scan========")
    this.testHbaseOperScanList
    this.testHbaseOperScanRDD
    this.testHbaseOperScanDF
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
