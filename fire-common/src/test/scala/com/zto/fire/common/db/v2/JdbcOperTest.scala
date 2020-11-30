package com.zto.fire.common.db.v2

import com.zto.fire.common.UnitTest
import com.zto.fire.common.anno.TestStep
import com.zto.fire.common.db.v2.bean.Student
import com.zto.fire.common.util.{DataSourceManager, PropUtils}
import org.apache.log4j.{Level, Logger}
import org.junit.{Before, Test}
import org.junit.Assert._

import scala.collection.JavaConversions._

/**
 * 用于测试JdbcOper相关API
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-30 14:23
 */
class JdbcOperTest extends UnitTest {
  private var jdbc: JdbcOper = _
  private var jdbc3: JdbcOper = _

  @Before
  def init: Unit = {
    PropUtils.load("JdbcOperTest")
    this.jdbc = JdbcOper()
    this.jdbc3 = JdbcOper(keyNum = 3)
  }

  @Test
  @TestStep(step = 1, desc = "jdbc CRUD测试")
  def testCRUD: Unit = {
    val studentName = "fire_test"

    val deleteSql = "delete from spark_test where name=?"
    this.jdbc.executeUpdate(deleteSql, Seq(studentName))
    this.jdbc3.executeUpdate(deleteSql, Seq(studentName))

    val selectSql = "select * from spark_test where name=?"
    val studentList1 = this.jdbc.executeQuery(selectSql, Seq(studentName), classOf[Student])
    val studentList3 = this.jdbc3.executeQuery(selectSql, Seq(studentName), classOf[Student])
    assertEquals(studentList1.size, 0)
    assertEquals(studentList3.size, 0)

    val insertSql = "insert into spark_test(name, age, length) values(?, ?, ?)"
    this.jdbc.executeUpdate(insertSql, Seq(studentName, 10, 10.3))
    this.jdbc3.executeUpdate(insertSql, Seq(studentName, 10, 10.3))

    val studentList11 = this.jdbc.executeQuery(selectSql, Seq(studentName), classOf[Student])
    val studentList33 = this.jdbc3.executeQuery(selectSql, Seq(studentName), classOf[Student])
    assertEquals(studentList11.size, 1)
    assertEquals(studentList33.size, 1)

    for (i <- 1 to 5) {
      DataSourceManager.get.foreach(t => {
        t._2.foreach(source => {
          println("数据源：" + t._1.toString + " " + source)
        })
      })
      println("=====================================")
      Thread.sleep(10000)
    }
  }
}
