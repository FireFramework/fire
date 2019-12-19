package com.zto.fire.demo

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.demo.bean.Student
import com.zto.fire.core.ext.SparkExt._

/**
 * 用于测试scala代码
 *
 * @author ChengLong 2019-9-4 13:39:16
 */
object ScalaTest extends BaseSparkCore {

  override def process: Unit = {

  }

    def main(args: Array[String]): Unit = {
      this.init()
      this.stop
    }

  }
