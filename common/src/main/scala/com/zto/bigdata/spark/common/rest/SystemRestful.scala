package com.zto.bigdata.spark.common.rest

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.bigdata.spark.common.anno.Rest
import com.zto.bigdata.spark.common.bean.rest.ResultMsg
import com.zto.bigdata.spark.common.bean.rest.spark.SparkInfo
import com.zto.bigdata.spark.common.core.BaseSpark
import com.zto.bigdata.spark.common.enu.ErrorCode
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util._
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

  // 系统预定义接口注册
  {
    this.baseSpark.restfulRegister
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/kill", kill))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/cancelJob", cancelJob))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/cancelStage", cancelStage))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/sql", sql))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/loadInfo", loadInfo))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/sparkInfo", sparkInfo))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/count", count))
  }

  @Rest("/system/count")
  def count(request: Request, response: Response): AnyRef = {
    this.baseSpark.count.value + ""
  }

  /**
    * 强制spark退出
    *
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/kill")
  def kill(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg()
    try {
      this.baseSpark.destroy
      ProcessUtil.executeCmds(s"yarn application -kill ${this.baseSpark.applicationId}", s"kill -9 ${SystemInfoUtils.getPid}")
      System.exit(0)
      msg.buildSuccess("任务停止成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.wrapLogError("kill job失败：" + e.getMessage)
        msg.buildError(e.getMessage, ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 取消job的执行
    *
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/cancelJob")
  def cancelJob(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg()
    try {
      val jobId = request.queryString()
      if (StringUtils.isNotBlank(jobId)) {
        this.baseSpark.sc.cancelJob(jobId.toInt, "被管控平台kill")
      }
      msg.buildSuccess("kill job 成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.wrapLogError("kill job失败：" + e.getMessage)
        msg.buildError(e.getMessage, ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 取消stage的执行
    *
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/cancelStage")
  def cancelStage(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg()
    try {
      val stageId = request.queryString()
      if (StringUtils.isNotBlank(stageId)) {
        this.baseSpark.sc.cancelStage(stageId.toInt, "被管控平台kill")
      }
      msg.buildSuccess("kill stage 成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.wrapLogError("kill stage失败：" + e.getMessage)
        msg.buildError(e.getMessage, ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 获取driver所在服务器的负载信息
    *
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/loadInfo")
  def loadInfo(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg
    try {
      msg.buildSuccess(SystemInfoUtils.getSystemLoadInfo, ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.wrapLogError("获取driver所在主机负载信息失败：" + e.getMessage)
        msg.buildError(e.getMessage, ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
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
    val msg = new ResultMsg()
    try {
      val sql = request.body()
      if (StringUtils.isBlank(sql) || sql.contains("alert") || sql.contains("drop") || sql.contains("ALERT") || sql.contains("DROP")) {
        return "sql不合法，暂不支持drop或alert语句"
      }
      if (this.baseSpark == null || this.baseSpark.spark == null) {
        return "系统正在初始化，请稍后再试"
      }
      msg.buildSuccess(this.baseSpark.spark.sql(sql).limit(1000).showString(), ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.wrapLogError("执行用户SQL失败：" + e.getMessage)
        msg.buildError(e.getMessage, ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
    * 获取当前的spark运行时信息
    *
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/sparkInfoBean")
  def sparkInfo(request: Request, response: Response): AnyRef = {
    val startTime = System.currentTimeMillis()
    val msg = new ResultMsg()
    try {
      if (this.sparkInfoBean == null) {
        this.sparkInfoBean = new SparkInfo
        this.sparkInfoBean.setAppName(this.baseSpark.appName)
        this.sparkInfoBean.setClassName(this.baseSpark.className)
        this.sparkInfoBean.setCommonVersion(PropUtils.getString("spark.common.version", "1.0.0"))
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
        this.sparkInfoBean.setTopics(PropUtils.getString(GlobalConstants.PropKeys.KAFKA_TOPICS, ""))
        this.sparkInfoBean.setBrokers(GlobalConstants.KafkaConf.kafkaBrokers())
        this.sparkInfoBean.setGroupId(PropUtils.getString(GlobalConstants.PropKeys.KAFKA_GROUP_ID, this.baseSpark.appName))
        this.sparkInfoBean.computeCpuMemory()
      }
      this.sparkInfoBean.setUptime(SparkUtils.runTime(this.baseSpark.startTime))
      this.sparkInfoBean.setBatchDuration(this.baseSpark.batchDuration + "")
      this.sparkInfoBean.setTimestamp(DateFormatUtils.formatCurrentDateTime())
      this.sparkInfoBean.setTimeCost(System.currentTimeMillis() - startTime)
      msg.buildSuccess(this.sparkInfoBean, ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.wrapLogError("获取spark info信息失败：" + e.getMessage)
        msg.buildError(e.getMessage, ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

}
