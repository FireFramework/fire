package com.zto.bigdata.spark.common.rest

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.bigdata.spark.common.anno.Rest
import com.zto.bigdata.spark.common.ext.BaseSpark
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import spark._

import scala.util.parsing.json.JSONObject

/**
  * 系统预定义的restful服务
  *
  * @author ChengLong 2019-3-16 10:16:38
  */
class SystemRestful(val baseSpark: BaseSpark) {

  // 系统预定义接口注册
  {
    this.baseSpark.restfulRegister
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/kill", kill))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/info", systemLoadInfo))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/conf", conf))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/version", version))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/master", master))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/applicationId", applicationId))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/applicationAttemptId", applicationAttemptId))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/ui", ui))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/pid", pid))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/uptime", uptime))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/startTime", startTime))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/sql", sql))
  }

  /**
    * 强制退出
    *
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/kill")
  def kill(request: Request, response: Response): AnyRef = {
    if (this.baseSpark.ssc == null) {
      this.baseSpark.spark.stop()
    } else {
      this.baseSpark.ssc.stop(true, false)
    }
    this.baseSpark.threadPool.shutdownNow()
    this.baseSpark.threadPoolSchedule.shutdownNow()
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
  @Rest("/system/systemLoadInfo")
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
  @Rest(value = "/system/sql", method = "post")
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
  @Rest("/system/conf")
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
  @Rest("/system/version")
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
  @Rest("/system/master")
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
  @Rest("/system/applicationId")
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
  @Rest("/system/applicationAttemptId")
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
  @Rest("/system/ui")
  def ui(request: Request, response: Response): AnyRef = {
    val line = new StringBuilder()
    this.baseSpark.webUI.split(",").foreach(url => {
      line.append(StringsUtils.hrefTag(url) + StringsUtils.brTag(""))
    })

    line.toString()
  }

  /**
    * 获取当前driver进程的pid
    *
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/pid")
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
  @Rest("/system/startTime")
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
  @Rest("/system/uptime")
  def uptime(request: Request, response: Response): AnyRef = {
    SparkUtils.runTime(this.baseSpark.startTime)
  }
}
