package com.zto.fire.core.rest

import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.bean.rest.spark.SparkInfo
import com.zto.fire.common.enu.{ErrorCode, RequestMethod}
import com.zto.fire.common.util._
import com.zto.fire.core.BaseSpark
import com.zto.fire.core.ext.SparkExt._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.Logging
import spark._

import scala.collection.JavaConversions

/**
  * 系统预定义的restful服务
  *
  * @author ChengLong 2019-3-16 10:16:38
  */
class SystemRestful(val baseSpark: BaseSpark) extends Logging {
  private var sparkInfoBean: SparkInfo = _
  private val peripheral = "rest"

  // 系统预定义接口注册
  {
    this.baseSpark.restfulRegister
      .addRest(RestCase(RequestMethod.DELETE.toString, s"/system/kill", kill))
      .addRest(RestCase(RequestMethod.DELETE.toString, s"/system/cancelJob", cancelJob))
      .addRest(RestCase(RequestMethod.DELETE.toString, s"/system/cancelStage", cancelStage))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/sql", sql))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/loadInfo", loadInfo))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/sparkInfo", sparkInfo))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/count", count))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/log", log))
  }


  @Rest("/system/count")
  def count(request: Request, response: Response): AnyRef = {
    this.baseSpark.acc.getCounter + ""
  }

  /**
    * 获取运行时日志
    */
  @Rest("/system/log")
  def log(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      // 用户身份校验
      if (!EncryptUtils.checkPermission(json, this.baseSpark.className)) {
        this.logFire(s"[log] 非法请求：用户身份校验失败！ip=${request.ip()} json=$json", this.peripheral)
        return msg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip()}", ErrorCode.ERROR)
      }

      val logs = new StringBuilder("[")
      JavaConversions.asScalaIterator(this.baseSpark.acc.getLog.iterator()).foreach(log => {
        logs.append(log + ",")
      })

      // 参数校验与参数获取
      val clear = JSONUtils.getValue(json, "clear", false)
      if (clear) this.baseSpark.acc.logAccumulator.reset

      if (logs.length > 0 && logs.endsWith(",")) {
        this.logFire(s"[log] 日志获取成功：json=$json", this.peripheral)
        msg.buildSuccess(logs.substring(0, logs.length - 1) + "]", "日志获取成功")
      } else {
        this.logFire(s"[log] 日志记录数为空：json=$json", this.peripheral)
        msg.buildError("日志记录数为空", ErrorCode.NOT_FOUND)
      }
    } catch {
      case e => {
        this.logFire(s"[log] 日志获取失败：json=$json", this.peripheral, throwable = e)
        msg.buildError("日志获取失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * kill 当前 Spark 任务
    */
  @Rest("/system/kill")
  def kill(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      // 用户身份校验
      if (!EncryptUtils.checkPermission(json, this.baseSpark.className)) {
        this.logFire(s"[kill] 非法请求：用户身份校验失败！ip=${request.ip} json=$json", this.peripheral)
        return msg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip}", ErrorCode.ERROR)
      }

      // 参数校验与参数获取
      val stopGracefully = JSONUtils.getValue(json, "stopGracefully", true)
      this.baseSpark.shutdown(stopGracefully)
      ProcessUtil.executeCmds(s"yarn application -kill ${this.baseSpark.applicationId}", s"kill -9 ${SystemInfoUtils.getPid}")
      this.logFire(s"[kill] kill任务成功：json=$json", this.peripheral)
      System.exit(0)
      msg.buildSuccess("任务停止成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[kill] 执行kill任务失败：json=$json", this.peripheral, throwable = e)
        msg.buildError("执行kill任务失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 取消job的执行
    */
  @Rest("/system/cancelJob")
  def cancelJob(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      // 用户身份校验
      if (!EncryptUtils.checkPermission(json, this.baseSpark.className)) {
        this.logFire(s"[cancelJob] 非法请求：用户身份校验失败！ip=${request.ip} json=$json", this.peripheral)
        return msg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip}", ErrorCode.ERROR)
      }

      // 参数校验与参数获取
      val jobId = JSONUtils.getValue(json, "id", -1)
      if (jobId == null || jobId <= 0) {
        this.logFire(s"[cancelJob] 参数不合法：json=$json", this.peripheral)
        return msg.buildError(s"参数不合法：json=$json", ErrorCode.ERROR)
      }

      this.baseSpark.sc.cancelJob(jobId, s"被管控平台kill：${DateFormatUtils.formatCurrentDateTime()}")
      this.logFire(s"[cancelJob] kill job成功：json=$json")
      msg.buildSuccess("kill job 成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[cancelJob] kill job失败：json=$json", this.peripheral, throwable = e)
        msg.buildError("kill job失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 取消stage的执行
    */
  @Rest("/system/cancelStage")
  def cancelStage(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      // 用户身份校验
      if (!EncryptUtils.checkPermission(json, this.baseSpark.className)) {
        this.logFire(s"[cancelStage] 非法请求：用户身份校验失败！ip=${request.ip} json=$json", this.peripheral)
        return msg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip}", ErrorCode.ERROR)
      }

      // 参数校验与参数获取
      val stageId = JSONUtils.getValue(json, "id", -1)
      if (stageId == null || stageId <= 0) {
        this.logFire(s"[cancelStage] 参数不合法：json=$json", this.peripheral)
        return msg.buildError(s"参数不合法：json=$json", ErrorCode.ERROR)
      }

      this.baseSpark.sc.cancelStage(stageId, s"被管控平台kill：${DateFormatUtils.formatCurrentDateTime()}")
      this.logFire(s"[cancelStage] kill stage[${stageId}] 成功：json=$json", this.peripheral)
      msg.buildSuccess("kill stage 成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[cancelStage] kill stage失败：json=$json", this.peripheral, throwable = e)
        msg.buildError("kill stage失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 获取driver所在服务器的负载信息
    */
  @Rest("/system/loadInfo")
  def loadInfo(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      // 用户身份校验
      if (!EncryptUtils.checkPermission(json, this.baseSpark.className)) {
        this.logFire(s"[loadInfo] 非法请求：用户身份校验失败！ip=${request.ip} json=$json", this.peripheral)
        return msg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip} json=$json", ErrorCode.ERROR)
      }

      msg.buildSuccess(SystemInfoUtils.getSystemLoadInfo, ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[loadInfo] 获取driver所在主机负载信息失败：json=$json", this.peripheral, throwable = e)
        msg.buildError("获取driver所在主机负载信息失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 用于执行sql语句
    */
  @Rest(value = "/system/sql", method = "post")
  def sql(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      // 用户身份校验
      if (!EncryptUtils.checkPermission(json, this.baseSpark.className)) {
        this.logFire(s"[sql] 非法请求：用户身份校验失败！ip=${request.ip} json=$json", this.peripheral)
        return msg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip} json=$json", ErrorCode.ERROR)
      }

      // 参数校验与参数获取
      val sql = JSONUtils.getValue(json, "sql", "")

      if (StringUtils.isBlank(sql) || sql.toUpperCase.contains("alert") || sql.toUpperCase.contains("drop") || sql.toLowerCase.contains("delete") || sql.toLowerCase.contains("create") || sql.toLowerCase.contains("insert")) {
        this.logFire(s"[sql] sql不合法：json=$json", this.peripheral)
        return msg.buildError(s"sql不合法", ErrorCode.ERROR)
      }

      if (this.baseSpark == null || this.baseSpark.spark == null) {
        this.logFire(s"[sql] 系统正在初始化，请稍后再试：json=$json", this.peripheral)
        return "系统正在初始化，请稍后再试"
      }
      this.logFire(s"[sql] 执行用户sql成功：json=$json", this.peripheral)
      msg.buildSuccess(this.baseSpark.spark.sql(sql).limit(1000).showString(), ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[sql] 执行用户sql失败：json=$json", this.peripheral, throwable = e)
        msg.buildError("执行用户sql失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 获取当前的spark运行时信息
    */
  @Rest("/system/sparkInfoBean")
  def sparkInfo(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      // 用户身份校验
      if (!EncryptUtils.checkPermission(json, this.baseSpark.className)) {
        this.logFire(s"[sparkInfo] 非法请求：用户身份校验失败！ip=${request.ip} json=$json", this.peripheral)
        return msg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip}", ErrorCode.ERROR)
      }

      if (this.sparkInfoBean == null) {
        this.sparkInfoBean = new SparkInfo
        this.sparkInfoBean.setAppName(this.baseSpark.appName)
        this.sparkInfoBean.setClassName(this.baseSpark.className)
        this.sparkInfoBean.setFireVersion(PropUtils.getString("spark.fire.version", "1.0.0"))
        this.sparkInfoBean.setConf(JavaConversions.mapAsJavaMap(this.baseSpark.spark.conf.getAll))
        this.sparkInfoBean.setVersion(this.baseSpark.sc.version)
        this.sparkInfoBean.setMaster(this.baseSpark.sc.master)
        this.sparkInfoBean.setApplicationId(this.baseSpark.sc.applicationId)
        this.sparkInfoBean.setApplicationAttemptId(this.baseSpark.sc.applicationAttemptId.getOrElse(""))
        this.sparkInfoBean.setUi(this.baseSpark.webUI)
        this.sparkInfoBean.setPid(SystemInfoUtils.getPid)
        this.sparkInfoBean.setStartTime(DateFormatUtils.formatUnixDateTime(this.baseSpark.startTime * 1000))
        this.sparkInfoBean.setExecutorMemory(this.baseSpark.sc.getConf.get("spark.executor.memory", "1"))
        this.sparkInfoBean.setExecutorInstances(this.baseSpark.sc.getConf.get("spark.executor.instances", "1"))
        this.sparkInfoBean.setExecutorCores(this.baseSpark.sc.getConf.get("spark.executor.cores", "1"))
        this.sparkInfoBean.setDriverCores(this.baseSpark.sc.getConf.get("spark.driver.cores", "1"))
        this.sparkInfoBean.setDriverMemory(this.baseSpark.sc.getConf.get("spark.driver.memory", "1"))
        this.sparkInfoBean.setDriverMemoryOverhead(this.baseSpark.sc.getConf.get("spark.yarn.driver.memoryOverhead", "0"))
        this.sparkInfoBean.setDriverHost(this.baseSpark.sc.getConf.get("spark.driver.host", "0"))
        this.sparkInfoBean.setDriverPort(this.baseSpark.sc.getConf.get("spark.driver.port", "0"))
        this.sparkInfoBean.setRestPort(this.baseSpark.restPort.toString)
        this.sparkInfoBean.setExecutorMemoryOverhead(this.baseSpark.sc.getConf.get("spark.yarn.executor.memoryOverhead", "0"))
        this.sparkInfoBean.setProperties(PropUtils.cover)
        this.sparkInfoBean.computeCpuMemory()
      }
      this.sparkInfoBean.setUptime(DateFormatUtils.runTime(this.baseSpark.startTime))
      this.sparkInfoBean.setBatchDuration(this.baseSpark.batchDuration + "")
      this.sparkInfoBean.setTimestamp(DateFormatUtils.formatCurrentDateTime())
      this.logFire(s"[sparkInfo] 获取spark信息成功：json=$json", this.peripheral)
      msg.buildSuccess(this.sparkInfoBean, ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[sparkInfo] 获取spark信息失败：json=$json", this.peripheral, throwable = e)
        msg.buildError("获取spark信息失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

}
