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
  }
}
