package com.zto.fire.common.db.v2

import java.util.Random
import java.util.concurrent.TimeUnit

import com.codahale.metrics.jvm.{FileDescriptorRatioGauge, GarbageCollectorMetricSet, MemoryUsageGaugeSet, ThreadStatesGaugeSet}
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.zto.fire.common.anno.{Internal, TestStep}
import com.zto.fire.common.db.v2.bean.Student
import com.zto.fire.common.util.PropUtils
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConversions

/**
 * 用于单元测试HBaseOper中的API
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-13 15:06
 */
class HBaseOperTest {
  val tableName = "fire_test_1"
  val tableName2 = "fire_test_2"
  var hbase: HBaseOper = null
  var hbase2: HBaseOper = null
  val metrics = new MetricRegistry()

  @Before
  def init: Unit = {
    PropUtils.load("HBaseOperTest")
    this.hbase = HBaseOper()
    this.hbase2 = HBaseOper(keyNum = 2)
  }

  /**
   * 用于测试以下api：
   * 1. 判断表是否存在
   * 2. disable 表
   * 3. create 表
   */
  @Test
  @TestStep(step = 1, desc = "创建表API测试")
  def testDDL: Unit = this.createTestTable

  /**
   * 测试插入多条记录
   */
  @Test
  @TestStep(step = 2, desc = "增删改查API测试")
  def testInsert: Unit = {
    this.hbase.truncateTable(this.tableName)
    // 批量插入
    val studentList = Student.build(5)
    this.hbase.insert(this.tableName, JavaConversions.asScalaBuffer(studentList): _*)

    // get操作
    println("===========get=============")
    val rowKeyList = (1 to 5).map(i => i.toString)
    val getStudentList = this.hbase.get(this.tableName, classOf[Student], rowKeyList: _*)
    assertEquals(getStudentList.size, 5)
    getStudentList.foreach(println)
    val getOne = this.hbase.get(this.tableName, classOf[Student], HBaseOper.buildGet("1"))
    assertEquals(getOne.size, 1)

    println("===========scan=============")
    val scanList = this.hbase.scan(this.tableName, classOf[Student], "1", "3")
    assertEquals(scanList.size, 2)
    scanList.foreach(println)
  }

  /**
   * 测试跨集群支持
   */
  @Test
  @TestStep(step = 3, desc = "多集群测试")
  def testMultiCluster: Unit = {
    this.hbase.truncateTable(this.tableName)
    this.hbase2.truncateTable(this.tableName2)
    val studentList1 = Student.build(5)
    this.hbase.insert(this.tableName, JavaConversions.asScalaBuffer(studentList1): _*)
    val scanStudentList1 = this.hbase.scan(this.tableName, classOf[Student], "1", "6")
    assertEquals(scanStudentList1.size, 5)
    val studentList2 = Student.build(3)
    this.hbase2.insert(this.tableName2, JavaConversions.asScalaBuffer(studentList2): _*)
    val scanStudentList2 = this.hbase2.scan(this.tableName2, classOf[Student], "1", "6")
    assertEquals(scanStudentList2.size, 3)
  }

  /**
   * 测试多版本插入
   * 注：多版本需要在Student类上声明@HConfig注解：@HConfig(nullable = true, multiVersion = true)
   */
  @Test
  @TestStep(step = 4, desc = "多版本测试")
  def testMultiInsert: Unit = {
    this.hbase2.truncateTable(this.tableName2)
    val studentList = Student.build(5)
    this.hbase2.insert(this.tableName2, JavaConversions.asScalaBuffer(studentList): _*)
    val students = this.hbase2.get(this.tableName2, classOf[Student], "1", "2")
    students.foreach(println)
  }

  /**
   * 创建必要的表信息
   */
  @Internal
  private def createTestTable: Unit = {
    if (this.hbase.isExists(this.tableName)) this.hbase.dropTable(this.tableName)
    assertEquals(this.hbase.isExists(this.tableName), false)
    this.hbase.createTable(this.tableName, "info", "data")
    assertEquals(this.hbase.isExists(this.tableName), true)

    if (this.hbase2.isExists(this.tableName2)) this.hbase2.dropTable(this.tableName2)
    assertEquals(this.hbase2.isExists(this.tableName2), false)
    this.hbase2.createTable(this.tableName2, "info")
    assertEquals(this.hbase2.isExists(this.tableName2), true)
  }

  @Test
  def testMeter: Unit = {
    val reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build
    reporter.start(1, TimeUnit.SECONDS)

    val requests = metrics.meter("requests")
    (1 to 100).foreach(i => {
      requests.mark()
      Thread.sleep(10)
    })
    Thread.sleep(1000)
  }

  @Test
  def testHistogram: Unit = {
    val reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build
    reporter.start(1, TimeUnit.SECONDS)

    val resultCounts = metrics.histogram(MetricRegistry.name(classOf[HBaseOperTest], "result-counts"))
    val random = new Random()
    (1 to 1000).foreach(i => {
      resultCounts.update(random.nextInt(100))
      Thread.sleep(10)
    })
    Thread.sleep(1000)
  }

  @Test
  def testJvm: Unit = {
    val reporter = ConsoleReporter.forRegistry(metrics)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build
    reporter.start(3, TimeUnit.SECONDS)

    metrics.register("jvm.gc", new GarbageCollectorMetricSet())
    metrics.register("jvm.memroy", new MemoryUsageGaugeSet())
    metrics.register("jvm.thread-states", new ThreadStatesGaugeSet())
    metrics.register("jvm.fd.usage", new FileDescriptorRatioGauge())

    Thread.sleep(100000)
  }
}
