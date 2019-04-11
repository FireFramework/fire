package com.zto.bigdata.spark.common.rest

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.bigdata.spark.common.ext.BaseSpark
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import spark._

import scala.util.parsing.json.JSONObject

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
      .addRest(Rest(RequestMethod.GET.toString, s"/system/conf", conf))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/version", version))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/master", master))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/applicationId", applicationId))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/applicationAttemptId", applicationAttemptId))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/ui", ui))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/pid", pid))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/uptime", uptime))
      .addRest(Rest(RequestMethod.GET.toString, s"/system/startTime", startTime))
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
    ProcessUtil.execAndWaitFor(s"kill -9 ${SystemInfoUtils.getPid}")
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

  /**
    * 获取当前的sparkConf信息
    *
    * @param request
    * @param response
    * @return
    */
  def conf(request: Request, response: Response): AnyRef = {
    JSONObject(this.baseSpark.spark.conf.getAll)
  }

  /**
    * 获取spark任务版本信息
    *
    * @param request
    * @param response
    * @return
    */
  def version(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.version
  }

  /**
    * 获取spark任务的master信息
    *
    * @param request
    * @param response
    * @return
    */
  def master(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.master
  }

  /**
    * 获取spark任务的applicationId信息
    *
    * @param request
    * @param response
    * @return
    */
  def applicationId(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.applicationId
  }

  /**
    * 获取spark任务的applicationAttemptId信息
    *
    * @param request
    * @param response
    * @return
    */
  def applicationAttemptId(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.applicationAttemptId
  }

  /**
    * 获取spark任务的webUI地址信息
    *
    * @param request
    * @param response
    * @return
    */
  def ui(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.uiWebUrl.get
  }

  /**
    * 获取当前driver进程的pid
    *
    * @param request
    * @param response
    * @return
    */
  def pid(request: Request, response: Response): AnyRef = {
    SystemInfoUtils.getPid
  }

  /**
    * 获取当前任务启动时间
    *
    * @param request
    * @param response
    * @return
    */
  def startTime(request: Request, response: Response): AnyRef = {
    DateFormatUtils.formatUnixDateTime(this.baseSpark.startTime * 1000)
  }

  /**
    * 获取当前任务的运行总时长
    *
    * @param request
    * @param response
    * @return
    */
  def uptime(request: Request, response: Response): AnyRef = {
    SparkUtils.runTime(this.baseSpark.startTime)
  }
}
