package com.zto.bigdata.spark.common.rest

import java.util.concurrent.ExecutorService

import com.zto.bigdata.spark.common.anno.Rest
import com.zto.bigdata.spark.common.util.{GlobalConstants, ReflectionUtils, SystemInfoUtils}
import spark.{Request, Response, Route, Spark}
import com.zto.bigdata.spark.common.ext.SparkExt._

import scala.collection.JavaConversions
import scala.collection.mutable._

/**
  * Spark的restful服务注册
  *
  * @author ChengLong 2019-3-16 09:56:56
  */
class RestfulRegister(val threadPool: ExecutorService) {
  private val restList = ListBuffer[RestCase]()
  private var port: Integer = _

  /**
    * 注册新的rest接口
    *
    * @param rest
    * rest的封装信息
    * @return
    */
  def addRest(rest: RestCase): this.type = {
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

  /**
    * 扫描@Rest，并注册
    */
  def registerRestful(): Unit = {
    val restClassList = ReflectionUtils.scanAnnotation("com.zto", classOf[Rest])
    if (restClassList != null && restClassList.size() > 0) {
      JavaConversions.asScalaBuffer(restClassList).foreach(clazz => {
        if (clazz != null) {
          val methods = clazz.getDeclaredMethods
          if (methods != null && methods.size > 0) {
            methods.foreach(method => {
              method.setAccessible(true)
              val restAnno = method.getAnnotation(classOf[Rest])
              if (restAnno != null) {
                // this.addRest(RestCase(restAnno.method(), restAnno.value(), ))
                println(method.getName)
              }
            })
          }
        }
      })
    }
  }
}
