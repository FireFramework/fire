package com.zto.fire.examples.flink.connector.hbase

import com.zto.fire._
import org.apache.flink.api.scala._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{HBase, HBase2, HBase3, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Checkpoint
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.println
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.hadoop.hbase.client.Get

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * flink hbase sink
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-5-25 16:32:50
 */
@Checkpoint(30)
@HBase("fat")
@HBase2("fat") // 对应keyNum=2的Hbase集群地址
@HBase3("fat") // 对应keyNum=3的Hbase集群地址
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HBaseTest extends FlinkStreaming {
  lazy val tableName = "fire_test_1"
  lazy val tableName2 = "fire_test_2"
  lazy val tableName3 = "fire_test_3"
  lazy val tableName5 = "fire_test_5"
  lazy val tableName6 = "fire_test_6"
  lazy val tableName7 = "fire_test_7"
  lazy val tableName8 = "fire_test_8"
  lazy val tableName9 = "fire_test_9"
  lazy val tableName10 = "fire_test_10"
  lazy val tableName11 = "fire_test_11"
  lazy val tableName12 = "fire_test_12"
  lazy val tableName13 = "fire_test_13"
  lazy val tableName14 = "fire_test_14"
  lazy val tableName15 = "fire_test_15"
  lazy val tableName16 = "fire_test_16"

  /**
   * table的hbase sink
   */
  def testTableHBaseSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.flink.sqlQuery("select id, name, age from student group by id, name, age")
    // 方式一、自动将row转为对应的JavaBean
    // 注意：table对象上调用hbase api，需要指定泛型
    table.hbasePutTable[Student](this.tableName).setParallelism(1)
    this.fire.hbasePutTable[Student](table, this.tableName2, keyNum = 2)

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2[Student](this.tableName3)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.fire.hbasePutTable2[Student](table, this.tableName5, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * table的hbase sink
   */
  def testTableHBaseSink2(stream: DataStream[Student]): Unit = {
    val table = this.fire.sqlQuery("select id, name, age from student group by id, name, age")

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2(this.tableName6)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.flink.hbasePutTable2(table, this.tableName7, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink(stream: DataStream[Student]): Unit = {
    // 方式一、DataStream中的数据类型为HBaseBaseBean的子类
    // stream.hbasePutDS(this.tableName)
    this.fire.hbasePutDS[Student](stream, this.tableName8)

    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName9, keyNum = 2)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName10)(value => value)
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink2(stream: DataStream[Student]): Unit = {
    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName11)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName12, keyNum = 2)(value => value)
  }

  /**
   * hbase的基本操作
   */
  def testHBase: Unit = {
    // get操作
    val getList = ListBuffer(HBaseConnector.buildGet("1"))
    val student = HBaseConnector.get[Student](this.tableName, getList, 1)
    if (student != null) println(JSONUtils.toJSONString(student))
    // scan操作
    val studentList = HBaseConnector.scan[Student](this.tableName, HBaseConnector.buildScan("0", "9"), 1)
    if (studentList != null) println(JSONUtils.toJSONString(studentList))
    // delete操作
    HBaseConnector.deleteRows(this.tableName, Seq("1"))
  }


  /**
   * 多线程并发 Put DataStream
   */
  def testStreamHBasePutAsync(stream: DataStream[Student], threadNum: Int): Unit = {
    this.fire.hbasePutDSAsync[Student](stream, this.tableName14, threadNum = threadNum)
  }

  /**
   * 多线程并发 Put Table
   */
  def testTableHBasePutAsync(stream: DataStream[Student], threadNum: Int): Unit = {
    stream.createOrReplaceTempView("student_async_df")
    val table = this.fire.sqlQuery("select id, name, age from student_async_df group by id, name, age")
    table.hbasePutTableAsync[Student](this.tableName15, threadNum = 2).setParallelism(1)
  }

  /**
   * 多线程并发 Put DataStream2
   */
  def testStreamHBasePutAsync2(stream: DataStream[Student], threadNum: Int): Unit = {
    stream.hbasePutDSAsync2(this.tableName16, threadNum = 2)(value => value)
  }

  /**
   * stream / table 多线程 sink
   */
  def testStreamHBaseSinkAsync(stream: DataStream[Student], threadNum: Int): Unit = {
    stream.hbasePutDSAsync2(this.tableName13, threadNum = threadNum)(value => value)
  }

  /**
   * table 多线程 sink
   */
  def testTableHBaseSinkAsync(stream: DataStream[Student], threadNum: Int): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select id, name, age from student group by id, name, age")
    table.hbasePutTableAsync[Student](this.tableName15, threadNum = threadNum).setParallelism(1)
    table.hbasePutTableAsync2[Student](this.tableName16, keyNum = 2, threadNum = threadNum) {
      row => new Student(row.getField(0).toString.toLong, row.getField(1).toString, row.getField(2).toString.toInt)
    }
  }

  /**
   * 多线程并发 Get
   */
  def testHbaseGetListAsync(tableName: String, threadNum: Int): Unit = {
    println("===========testHbaseGetListAsync===========")
    val rowKeys = Seq("1", "2", "3", "5", "6")
    val studentList = this.fire.hbaseGetListAsync2[Student](tableName, threadNum, rowKeys)
    studentList.foreach(println)

    val getList = ListBuffer[Get]()
    rowKeys.foreach(rowKey => getList += new Get(rowKey.getBytes(StandardCharsets.UTF_8)))
    val studentList2 = this.fire.hbaseGetListAsync[Student](tableName, threadNum, getList)
    studentList2.foreach(println)
  }

  /**
   * 多线程并发 Scan
   */
  def testHbaseScanListAsync(tableName: String, threadNum: Int): Unit = {
    println("===========testHbaseScanListAsync===========")
    val list = this.fire.hbaseScanListAsync2[Student](tableName, threadNum, "1", "6")
    list.foreach(println)
  }

  private def studentStream(parallelism: Int = 2): DataStream[Student] = {
    this.fire.parallelize(Student.newStudentList().asScala.toSeq).setParallelism(parallelism)
  }

  override def process: Unit = {
    val stream = this.fire.createRandomIntStream(1).flatMap(t => Student.newStudentList()).setParallelism(1)
    /*HBaseConnector.truncateTable(this.tableName)
    HBaseConnector.truncateTable(this.tableName2)
    HBaseConnector.truncateTable(this.tableName3)
    HBaseConnector.truncateTable(this.tableName5)
    this.testTableHBaseSink(stream)
    this.testStreamHBaseSink(stream)
    this.testStreamHBaseSink2(stream)
    this.testTableHBaseSink2(stream)
    this.testHBase*/

    // 多线程 API 测试
    println("------------tableName13----------------")
    this.testStreamHBaseSinkAsync(stream, 3)
    this.testHbaseGetListAsync(this.tableName13, 2)
    this.testHbaseScanListAsync(this.tableName13, 2)

    // stream/table 异步 sink 在 job 启动后写入，Get/Scan 前用集合并发 Put 准备数据
    println("------------tableName14----------------")
    this.fire.hbasePutListAsync[Student](this.tableName14, 2, Student.newStudentList().asScala)
    this.testHbaseGetListAsync(this.tableName14, 2)
    this.testHbaseScanListAsync(this.tableName14, 2)

    println("------------tableName15----------------")
    this.fire.hbasePutListAsync[Student](this.tableName15, 2, Student.newStudentList().asScala)
    this.testHbaseGetListAsync(this.tableName15, 2)
    this.testHbaseScanListAsync(this.tableName15, 2)

    println("------------tableName16----------------")
    this.fire.hbasePutListAsync[Student](this.tableName16, 2, Student.newStudentList().asScala)
    this.testHbaseGetListAsync(this.tableName16, 2)
    this.testHbaseScanListAsync(this.tableName16, 2)
  }
}
