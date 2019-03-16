package com.zto.bigdata.spark

import spark.{Request, Response, Route, Spark}

import scala.collection.mutable._

class RestfulRegister {
  private val restList = ListBuffer[Rest]()

  case class Rest(method: String, path: String, fun: (Request, Response) => AnyRef)

  def addRest(rest: Rest): this.type = {
    this.restList += rest
    this
  }

  def port(port: Int): this.type = {
    Spark.port(port)
    this
  }

  def startRestServer: Unit = {
    this.restList.foreach(rest => {
      rest.method match {
        case "get" | "GET" => Spark.get(rest.path, new Route {
          override def handle(request: Request, response: Response): AnyRef = {
            rest.fun(request, response)
          }
        })
        case "post" | "POST" => Spark.post(rest.path, new Route {
          override def handle(request: Request, response: Response): AnyRef = {
            rest.fun(request, response)
          }
        })
        case "put" | "PUT" => Spark.put(rest.path, new Route {
          override def handle(request: Request, response: Response): AnyRef = {
            rest.fun(request, response)
          }
        })
        case "delete" | "DELETE" => Spark.delete(rest.path, new Route {
          override def handle(request: Request, response: Response): AnyRef = {
            rest.fun(request, response)
          }
        })
      }
    })
  }

  def count(request: Request, response: Response): AnyRef = {
    "spark restful"
  }

  /*def main(args: Array[String]): Unit = {
    RestfulRegister
      .port(10010)
      .addRest(RestfulRegister.Rest("get", "/count", this.count))
      .addRest(RestfulRegister.Rest("post", "/count2", this.count))
      .startRestServer
  }*/

}
