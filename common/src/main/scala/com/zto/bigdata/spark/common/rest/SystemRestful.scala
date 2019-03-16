package com.zto.bigdata.spark.common.rest

import com.zto.bigdata.spark.common.ext.BaseSpark
import spark.Spark.stop
import spark._

/**
  * 系统预定义的restful服务
  *
  * @author ChengLong 2019-3-16 10:16:38
  */
class SystemRestful(val baseSpark: BaseSpark) {

  /**
    * 注册系统预定义的restful服务
    */
  /*def register: Unit = {
    this.getClass.getDeclaredMethods.foreach(method => {
      method.setAccessible(true)
      val paramType = method.getGenericParameterTypes
      if(paramType != null && paramType.length == 2) {
        if(paramType(0) == classOf[spark.Request] && paramType(1) == classOf[spark.Request]) {
          this.baseSpark.restfulRegister.addRest(Rest(RequestMethod.GET.toString, s"/system/${method.getName}", kill))
        }
      }
    })
  }*/

  {
    this.baseSpark.restfulRegister.addRest(Rest(RequestMethod.GET.toString, s"/system/kill", kill))
  }

  /**
    * 强制退出
    * @param request
    * @param response
    * @return
    */
  def kill(request: Request, response: Response): AnyRef = {
    this.baseSpark.spark.stop()
    this.baseSpark.threadPool.shutdownNow()
    Spark.stop()
    System.exit(0)
    ""
  }
}
