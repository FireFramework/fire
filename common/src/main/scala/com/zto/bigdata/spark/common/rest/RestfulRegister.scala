package com.zto.bigdata.spark.common.rest

import java.util.concurrent.ExecutorService

import com.zto.bigdata.spark.common.util.{GlobalConstants, SystemInfoUtils}
import spark.{Request, Response, Route, Spark}

import scala.collection.mutable._

/**
  * Spark的restful服务注册
  *
  * @author ChengLong 2019-3-16 09:56:56
  */
class RestfulRegister(val threadPool: ExecutorService) {
  private val restList = ListBuffer[Rest]()
  private var port: Integer = _

  /**
    * 注册新的rest接口
    *
    * @param rest
    * rest的封装信息
    * @return
    */
  def addRest(rest: Rest): this.type = {
    this.restList += rest
    this
  }

  /**
    * rest占用的端口号
    *
    * @param port
    * 端口号
    * @return
    */
  def port(port: Int): this.type = {
    Spark.port(port)
    this.port = port
    this
  }

  /**
    * 注册并以子线程方式开启rest服务
    */
  def startRestServer: Unit = {
    if (this.port == null) {
      this.port(SystemInfoUtils.getRundomPort)
    }
    val restPrefix = s"http://${SystemInfoUtils.getIp}:${this.port}"

    this.threadPool.execute(new Runnable {
      override def run(): Unit = {
        restList.foreach(rest => {
          println(s"---------> start rest: ${GlobalConstants.PS1.wrap(restPrefix + rest.path, GlobalConstants.PS1.BLUE, GlobalConstants.PS1.UNDER_LINE)} successfully. <---------")
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
    })
  }
}
