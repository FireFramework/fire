package com.zto.fire.demo

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.demo.bean.Student
import com.zto.fire.core.ext.SparkExt._
import org.apache.spark.sql.api.java.UDF1

/**
 * 用于测试scala代码
 *
 * @author ChengLong 2019-9-4 13:39:16
 */
object ScalaTest extends BaseSparkCore {

  val anonfun1 = new Function1[Int, Int] {
    def apply(x: Int): Int = x + 1
  }

  def test(str: String): Int = 1

  override def process: Unit = {
    /*val func = UDF1[_, _].asInstanceOf[UDF1[Any, Any]].call(_: Any)
    this.spark.udf.register("test", func)*/

  }

    def main(args: Array[String]): Unit = {
      this.init()
      this.stop
    }

  }
