package com.zto.bigdata.spark.common.rest

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.bigdata.spark.common.anno.Rest
import com.zto.bigdata.spark.common.ext.BaseSpark
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.execution.command.CommandUtils
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
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/executorMemory", executorMemory))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/executorInstances", executorInstances))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/executorCores", executorCores))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/driverCores", driverCores))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/driverMemory", driverMemory))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/driverMemoryOverhead", driverMemoryOverhead))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/driverHost", driverHost))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/driverPort", driverPort))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/executorMemoryOverhead", executorMemoryOverhead))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/memory", memory))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/cpu", cpu))
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
    this.baseSpark.destroy
    ProcessUtil.executeCmds(s"yarn application -kill ${this.baseSpark.applicationId}", s"kill -9 ${SystemInfoUtils.getPid}")
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

  /**
    * 获取executor内存信息
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/executorMemory")
  def executorMemory(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.executor.memory", "1")
  }

  /**
    * 获取executor个数
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/executorInstances")
  def executorInstances(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.executor.instances", "1")
  }

  /**
    * 获取executor cpu数量
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/executorCores")
  def executorCores(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.executor.cores", "1")
  }

  /**
    * 获取driver cpu数量
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/driverCores")
  def driverCores(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.driver.cores", "1")
  }

  /**
    * 获取driver内存大小
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/driverMemory")
  def driverMemory(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.driver.memory", "1")
  }

  /**
    * 获取driver堆外内存大小
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/driverMemoryOverhead")
  def driverMemoryOverhead(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.yarn.driver.memoryOverhead", "0")
  }

  /**
    * 获取driver所在服务器ip
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/driverHost")
  def driverHost(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.driver.host", "0")
  }

  /**
    * 获取driver占用的端口号
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/driverPort")
  def driverPort(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.driver.port", "0")
  }

  /**
    * 获取堆外内存大小
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/executorMemoryOverhead")
  def executorMemoryOverhead(request: Request, response: Response): AnyRef = {
    this.baseSpark.sc.getConf.get("spark.yarn.executor.memoryOverhead", "0")
  }

  /**
    * 获取当前任务的总内存
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/memory")
  def memory(request: Request, response: Response): AnyRef = {
    // driver的总内存大小 + executor的总内存大小
    (this.driverMemory(request, response).toString.replace("g", "").toInt + this.driverMemoryOverhead(request, response).toString.replace("g", "").toInt + this.executorInstances(request, response).toString.toInt * (this.executorMemory(request, response).toString.replace("g", "").toInt + this.executorMemoryOverhead(request, response).toString.replace("g", "").toInt)).toString
  }

  /**
    * 获取当前任务的总cpu数量
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/cpu")
  def cpu(request: Request, response: Response): AnyRef = {
    // executor总数 * 每个executor的cpu数 + driver的cpu数
    (this.executorInstances(request, response).toString.toInt * this.executorCores(request, response).toString.toInt + this.driverCores(request, response).toString.toInt).toString
  }
}
