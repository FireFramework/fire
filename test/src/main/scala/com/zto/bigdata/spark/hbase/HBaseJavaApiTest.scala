package com.zto.bigdata.spark.hbase

import java.util

import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.core.BaseSparkCore
import com.zto.bigdata.spark.common.db.HBaseOper
import com.zto.bigdata.spark.common.ext.SparkExt._
import org.apache.spark.sql.Encoders

import scala.collection.JavaConversions

/**
  * 在spark中使用java 同步 api 的方式读写hbase表
  *
  * @author ChengLong 2019-5-9 09:37:25
  */
object HBaseJavaApiTest extends BaseSparkCore {
  private val tableName1 = "zto_test_senda"
  private val tableName2 = "zto_test_senda2"
  private val tableName3 = "zto_test_senda3"
  private val tableName4 = "zto_test_senda4"

  /**
    * 使用Java Api 方式对HBase多版本进行读写
    * 注：适用于Java程序
    */
  def testJavaMultiVersion(): Unit = {
    (1 to 60).foreach(x => {
      val list = new util.ArrayList[Student]()
      list.add(new Student(1L, s"root_$x", x))
      // 多版本插入，会将数据转为json存储
      HBaseOper.insertMultiVersions(this.tableName1, list)
    })
    // 多版本数据读取，指定6表示读取最近6个版本，若需读取全部版本，则此参数不填
    val studentLists = HBaseOper.getMultiVersions(this.tableName1, "1", classOf[Student])
    JavaConversions.asScalaBuffer(studentLists).foreach(println)
  }

  /**
    * 使用Java API方式对版本数为1的表进行读写
    * 注：适用于Java程序
    */
  def testJavaRW(): Unit = {
    val list = new util.ArrayList[Student]()
    list.add(new Student(1L, s"root1", 12))
    list.add(new Student(2L, s"root2", 22))
    // 单版本插入（hbase表版本数为1）
    HBaseOper.insert(this.tableName1, list)
    // 指定rowKey读取数据
    val student = HBaseOper.get(this.tableName1, "1", classOf[Student])
    println(student)
    println(HBaseOper.get(this.tableName1, "2", classOf[Student]))
    println(HBaseOper.get(this.tableName1, "3", classOf[Student]))
  }

  /**
    * 使用Java API的方式将rdd中的数据写入到hbase中
    */
  def testSparkWrite(): Unit = {
    // rdd数据写入到hbase中
    val studentRDD = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    studentRDD.hbaseOperPutRDD(this.tableName1)
    // dataFrame数据写入到hbase中
    /*val df = this.spark.createDataFrame(studentRDD, classOf[Student])
    df.hbaseInsertDF(this.tableName1, classOf[Student])*/
  }

  /**
    * spark scan HBase表记录
    */
  def testSparkScan(): Unit = {
    val rdd = this.spark.hbaseOperScanRDD2(this.tableName1, "1", "3", classOf[Student], true, 10)
    rdd.foreach(println)
    println("===========df==========")
    val df = this.spark.hbaseOperScanDF2(this.tableName1, "1", "3", classOf[Student], true, 10)
    df.show(100, false)
  }

  /**
    * 将get到的一个或多个版本映射为RDD或DataFrame
    */
  def testSparkGet(): Unit = {
    val rowKeyRDD = this.spark.parallelize(Seq("3"))
    val studentDF = rowKeyRDD.hbaseOperGetDF(this.tableName1, classOf[Student], true, 3)
    studentDF.show(100, false)
  }

  /**
    * 使用HBaseOper插入一个集合，可以是list、set等集合
    * 但集合的类型必须为HBaseBaseBean的子类
    */
  def testHbaseOperInsertList(): Unit = {
    val studentList = Student.buildStudentList()
    this.spark.hbaseOperPutList(this.tableName1, JavaConversions.asScalaBuffer(studentList))
  }

  /**
    * 使用HBaseOper插入一个rdd的数据
    * rdd的类型必须为HBaseBaseBean的子类
    */
  def testHbaseOperInsertRDD(): Unit = {
    val studentList = Student.buildStudentList()
    val studentRDD = this.spark.parallelize(JavaConversions.asScalaBuffer(studentList), 2)
    studentRDD.hbaseOperPutRDD(this.tableName2)
  }

  /**
    * 使用HBaseOper插入一个DataFrame的数据
    */
  def testHbaseOperInsertDF(): Unit = {
    val studentList = Student.buildStudentList()
    val studentDF = this.spark.createDataFrame(studentList, classOf[Student])
    studentDF.hbaseOperPutDF(this.tableName3, classOf[Student])
  }

  /**
    * 使用HBaseOper插入一个Dataset的数据
    * dataset的类型必须为HBaseBaseBean的子类
    */
  def testHbaseOperInsertDS(): Unit = {
    val studentList = Student.buildStudentList()
    val studentDS = this.spark.createDataset(JavaConversions.asScalaBuffer(studentList))(Encoders.bean(classOf[Student]))
    studentDS.hbaseOperPutDS(this.tableName4, classOf[Student])
  }

  /**
    * 使用HBaseOper get数据，并将结果以list方式返回
    */
  def testHbaseOperGetList(): Unit = {
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val studentList = this.spark.hbaseOperGetList2(this.tableName1, getList, classOf[Student])
    studentList.foreach(println)
  }

  /**
    * 使用HBaseOper get数据，并将结果以RDD方式返回
    */
  def testHbaseOperGetRDD: Unit = {
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.spark.parallelize(getList)
    val studentRDD = this.spark.hbaseOperGetRDD(this.tableName1, getRDD, classOf[Student])
    studentRDD.printEachPartition
  }

  /**
    * 使用HBaseOper get数据，并将结果以DataFrame方式返回
    */
  def testHbaseOperGetDF: Unit = {
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.spark.parallelize(getList)
    val studentDF = this.spark.hbaseOperGetDF(this.tableName1, getRDD, classOf[Student])
    studentDF.show(100, false)
  }

  /**
    * 使用HBaseOper get数据，并将结果以Dataset方式返回
    */
  def testHbaseOperGetDS: Unit = {
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.spark.parallelize(getList)
    val studentDS = this.spark.hbaseOperGetDS(this.tableName1, getRDD, classOf[Student])
    studentDS.show(100, false)
  }


  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    /*this.testHbaseOperInsertList
    this.testHbaseOperInsertRDD
    this.testHbaseOperInsertDF
    this.testHbaseOperInsertDS()
    this.testHbaseOperInsertList
    this.testHbaseOperGetList
    this.testHbaseOperGetRDD
    this.testHbaseOperGetDF
    this.testHbaseOperGetDS*/
  }

  def main(args: Array[String]): Unit = {
    this.init()

    this.spark.stop()
  }
}
