package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.common.ext.BaseSparkCore
import spark.{Request, Response, Route}
import spark.Spark._

object RestTest extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    // 设置端口号
    port(10010)
    this.init()
    this.runAsThread(this.rest)
    this.runAsThread(this.showTables)
  }

  def showTables: Unit = {
    this.spark.sql("show tables").show()
  }

  def rest: Unit = {
    get("/count", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        println(s"ip:${request.ip()} 调用/count接口")
        spark.sql("select count(1) from dw_sz_zto_site_senda_bills").show()
        return request.body()
      }
    })

    get("/select", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        println(s"ip:${request.ip()} 调用/select接口")
        spark.sql("select * from dw_sz_zto_site_senda_bills").show(10)
        return ""
      }
    })

    get("/sql", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        println(s"ip:${request.ip()} 调用/sql接口")
        spark.sql(request.body()).show(100)
        return "执行sql成功"
      }
    })

    get("/stop", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        sc.stop()
        threadPool.shutdownNow()
        println(sc.isStopped)
        stop()
        System.exit(0)
        return ""
      }
    })
  }

}
