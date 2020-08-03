package com.zto.fire.core.rest

import java.util.concurrent.ExecutorService

import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import com.zto.fire.common.enu.ErrorCode
import com.zto.fire.common.util.{EncryptUtils, PropUtils, ReflectionUtils, SystemInfoUtils}
import org.slf4j.LoggerFactory
import spark._

import scala.collection.JavaConversions
import scala.collection.mutable._

/**
  * Fire框架的restful服务注册
  *
  * @author ChengLong 2019-3-16 09:56:56
  */
class RestfulRegister(val threadPool: ExecutorService) {
  private val restList = ListBuffer[RestCase]()
  private var port: Integer = _
  private val logger = LoggerFactory.getLogger(this.getClass)
  private[this] lazy val mainClassName: String = FireFrameworkConf.driverClassName

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
    Spark.threadPool(FireFrameworkConf.restfulMaxThread, 2, -1)
    Spark.port(port)
    this.port = port
    this
  }

  /**
    * 注册并以子线程方式开启rest服务
    */
  def startRestServer: Unit = {
    if (!FireFrameworkConf.restEnable) return

    if (this.port == null) {
      this.port(SystemInfoUtils.getRundomPort)
    }
    val restPrefix = s"http://${SystemInfoUtils.getIp}:${this.port}"

    this.threadPool.execute(new Runnable {
      override def run(): Unit = {
        restList.foreach(rest => {
          if (FireFrameworkConf.fireRestUrlShow) logger.info(s"---------> start rest: ${FirePS1Conf.wrap(restPrefix + rest.path, FirePS1Conf.BLUE, FirePS1Conf.UNDER_LINE)} successfully. <---------")
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

        // 注册过滤器，用于进行权限校验
        Spark.before(new Filter {
          override def handle(request: Request, response: Response): Unit = {
            if (FireFrameworkConf.restFilter) {
              val msg = checkAuth(request)
              if (msg.getCode != null && ErrorCode.UNAUTHORIZED == msg.getCode) {
                Spark.halt(401, msg.toString)
              }
            }
          }
        })
      }
    })
  }

  /**
    * 通过header进行用户权限校验
    */
  private[fire] def checkAuth(request: Request): ResultMsg = {
    val msg = new ResultMsg
    val auth = request.headers("Authorization")
    try {
      if (!EncryptUtils.checkAuth(auth, this.mainClassName)) {
        this.logger.warn(s"非法请求：用户身份校验失败！ip=${request.ip()} auth=$auth")
        msg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip()}", ErrorCode.UNAUTHORIZED)
      }
    } catch {
      case e => {
        this.logger.error(s"非法请求：请检查请求参数！ip=${request.ip()} auth=$auth", e)
        msg.buildError(s"非法请求：请检查请求参数！ip=${request.ip()}", ErrorCode.UNAUTHORIZED)
      }
    }
    msg
  }


  /**
    * 扫描@Rest，并注册
    */
  def registerRestful(): Unit = {
    val restClassList = ReflectionUtils.scanAnnotation("com.zto", classOf[Rest])
    if (restClassList != null && restClassList.size() > 0) {
      JavaConversions.asScalaSet(restClassList).foreach(clazz => {
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
