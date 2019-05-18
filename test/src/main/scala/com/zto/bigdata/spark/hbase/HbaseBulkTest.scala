package com.zto.bigdata.spark.hbase

import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.core.BaseSparkCore
import com.zto.bigdata.spark.common.ext.SparkExt._
import org.apache.spark.sql.{Encoders, Row}

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
    * 使用id作为rowKey
    */
  val buildStudentRowKey = (row: Row) => {
    row.getAs("id").toString
  }

  /**
    * 使用bulk的方式将rdd写入到hbase
    */
  def testBulkPutRDD: Unit = {
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
    * 使用bulk的方式将Dataset写入到hbase
    */
  def testBulkPutDataset: Unit = {
    // 方式一：将DataFrame的数据写入到hbase中
    val rdd = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    val studentDataset = this.spark.createDataset(rdd)(Encoders.bean(classOf[Student]))
    studentDataset.hbaseBulkPutDataset(this.tableName)
    // 方式二：
    // this.spark.hbaseBulkPutDataset(this.tableName, studentDataset)
  }

  /**
    * 使用bulk的方式将DataFrame写入到hbase
    */
  def testBulkPutDF: Unit = {
    // 方式一：将DataFrame的数据写入到hbase中
    val rdd = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    val studentDF = this.spark.createDataFrame(rdd, classOf[Student])
    // studentDF.hbaseBulkPutDF(this.tableName, classOf[Student])
    // 方式二：multiVersion = true表示以多版本方式写入，多个字段会自动被转为json，对应的读取时会自动解析json为对象
    // this.spark.hbaseBulkPutDF(this.tableName, studentDF, classOf[Student], multiVersion = true)
    // 方式三：直接将Row类型的DataFrame写入到hbase，并通过buildStudentRowKey指定构造rowKey的规则
    // 该种方式更高效，中间不比经过JavaBean的转换
    // TODO: 本地测试环境报错，待线上测试
    studentDF.hbaseHadoopPutDFRow(this.tableName, buildStudentRowKey)
  }

  /**
    * 使用spark的方式将dataframe写入到hbase
    */
  def testHbaseHadoopPutDF: Unit = {
    // 方式一：将DataFrame的数据写入到hbase中
    val rdd = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    val studentDF = this.spark.createDataFrame(rdd, classOf[Student])
    // studentDF.hbaseHadoopPutDF(this.tableName, classOf[Student])
    // 方式二：TODO: 需在生产环境下测试，本地环境有问题
    this.spark.hbaseHadoopPutDF(this.tableName, studentDF, classOf[Student])
  }

  /**
    * 使用spark的方式将dataset写入到hbase
    */
  def testHbaseHadoopPutDataset: Unit = {
    // 方式一：将dataset的数据写入到hbase中
    val rdd = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    val studentDataset = this.spark.createDataset(rdd)(Encoders.bean(classOf[Student]))
    // studentDataset.hbaseHadoopPutDataset(this.tableName)
    // 方式二：TODO: 需在生产环境下测试，本地环境有问题
    this.spark.hbaseHadoopPutDataset(this.tableName, studentDataset)
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
    // this.testBulkPutRDD
    // this.testBulkPutDF
    // this.testBulkPutDataset
    this.testHbaseHadoopPutDataset
    this.testBulkDelete
    this.testBulkGet
    // this.testBulkScan
  }

  def main(args: Array[String]): Unit = {
    this.init()

    this.spark.stop()
  }

}
