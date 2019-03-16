package com.zto.bigdata.spark

import com.zto.bigdata.spark.bean.Test
import com.zto.bigdata.spark.common.rest.Rest
import com.zto.bigdata.spark.rest.RestTest2
import spark.Spark._
import spark.{Request, Response, Route}

import scala.reflect.runtime.universe._
import scala.reflect._

class Test1 {

  def count(request: Request, response: Response): AnyRef = {
    "hello world"
  }

  def getRest(fun: (Request, Response) => AnyRef): Unit = {
    get("", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        return fun(request, response)
      }
    })
  }


}

object Test {
  def main(args: Array[String]): Unit = {
    val va = typeOf[Test1].member(newTermName("getRest"))//.asMethod.paramss.head
    val ru = scala.reflect.runtime.universe
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = rm.reflect(Test)
    val methodSymbolHi = ru.typeOf[Test.type].decl(ru.TermName("getRest")).asMethod
    val methodHi = instanceMirror.reflectMethod(methodSymbolHi)
    methodHi()
  }
}
