package com.zto.bigdata.spark

import spark.{Request, Response, Route, Spark}
import spark.Spark._

object Test {

  def main(args: Array[String]): Unit = {
    get("/hello", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        return request.body()
      }
    })
  }

}
