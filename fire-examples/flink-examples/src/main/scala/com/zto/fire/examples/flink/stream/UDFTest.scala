package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.ScalarFunction

import scala.collection.JavaConversions

/**
 * 自定义udf测试
 *
 * @author ChengLong 2020年1月13日 10:36:39
 * @since 0.4.1
 */
object UDFTest extends BaseFlinkStreaming {
  override def process: Unit = {
    val dataset = this.env.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))

    //this.flink.registerDataStream("test", dataset)
    // 注册udf
    //this.flink.udf("appendFire", new AppendFire)
    // 在sql中使用自定义的udf
    this.flink.sql("select appendFire(name), appendFire(age) from test")

    this.env.execute("udf test")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}

/**
 * 自定义udf方式，继承自ScalarFunction，并定义名称为eval的方法
 */
class AppendFire extends ScalarFunction {

  /**
   * 为指定字段的值追加fire字符串
   *
   * @param field
   * 字段名称
   * @return
   * 追加fire字符串后的字符串
   */
  def eval(field: Any): Any = field + "->fire"
}