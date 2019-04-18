package com.zto.bigdata.spark

import com.zto.bigdata.spark.common.rest.RestCase
import spark.{Request, Response}

object LocalTest {

  def conf(fun: (Request, Response) => AnyRef): Unit = {
    fun
  }

  def main(args: Array[String]): Unit = {
    classOf[RestCase].getDeclaredMethods.foreach(method => {
      method.setAccessible(true)
      println("name=" + method.getName)
      method.getParameterTypes.foreach(x => {
        if (method.getName.contains("fun")) {
          print(x.getTypeParameters + " ")
        }
      })
    })
  }
}
