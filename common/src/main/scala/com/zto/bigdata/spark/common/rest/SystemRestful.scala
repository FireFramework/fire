package com.zto.bigdata.spark.common.rest

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.bigdata.spark.common.ext.BaseSpark
import com.zto.bigdata.spark.common.util.{GlobalConstants, SystemInfoUtils}
import org.apache.commons.lang3.StringUtils
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
    // 接口注册
    this.baseSpark.restfulRegister
      .addRest(Rest(RequestMethod.GET.toString, s"/system/kill", kill))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/info", systemLoadInfo))
      .addRest(Rest(RequestMethod.POST.toString, s"/system/sql", sql))
  }

  /**
    * 强制退出
    *
    * @param request
    * @param response
    * @return
    */
  def kill(request: Request, response: Response): AnyRef = {
    this.baseSpark.spark.stop()
    this.baseSpark.threadPool.shutdownNow()
    Spark.stop()
    System.exit(0)
    GlobalConstants.Status.SUCCESS
  }

  /**
    * 获取driver所在服务器的负载信息
    *
    * @param request
    * @param response
    * @return
    */
  def systemLoadInfo(request: Request, response: Response): AnyRef = {
    JSON.toJSONString(SystemInfoUtils.getSystemLoadInfo, SerializerFeature.PrettyFormat)
  }

  /**
    * 用于执行sql语句
    *
    * @param request
    * @param response
    * @return
    */
  def sql(request: Request, response: Response): AnyRef = {
    val sql = request.queryString()
    if (StringUtils.isNotBlank(sql) && this.baseSpark != null && this.baseSpark.spark != null) {
      this.baseSpark.spark.sql(sql).show(false)
    }
    GlobalConstants.Status.SUCCESS
  }
}
