package com.zto.bigdata.spark.common.rest

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.bigdata.spark.common.anno.Rest
import com.zto.bigdata.spark.common.bean.rest.SparkInfo
import com.zto.bigdata.spark.common.ext.BaseSpark
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import com.zto.bigdata.spark.common.ext.ScalaExt._
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
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/sql", sql))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/kill", kill))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/loadInfo", loadInfo))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/sparkInfo", sparkInfo))
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
  @Rest("/system/loadInfo")
  def loadInfo(request: Request, response: Response): AnyRef = {
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
    * 获取当前的spark运行时信息
    *
    * @param request
    * @param response
    * @return
    */
  @Rest("/system/sparkInfo")
  def sparkInfo(request: Request, response: Response): AnyRef = {
    val sparkInfo = new SparkInfo
    sparkInfo.setConf(this.baseSpark.spark.conf.getAll.toJavaMap)
    sparkInfo.setVersion(this.baseSpark.sc.version)
    sparkInfo.setMaster(this.baseSpark.sc.master)
    sparkInfo.setApplicationId(this.baseSpark.sc.applicationId)
    sparkInfo.setApplicationAttemptId(this.baseSpark.sc.applicationAttemptId.getOrElse(""))
    sparkInfo.setUi(this.baseSpark.webUI)
    sparkInfo.setPid(SystemInfoUtils.getPid)
    sparkInfo.setUptime(SparkUtils.runTime(this.baseSpark.startTime))
    sparkInfo.setStartTime(DateFormatUtils.formatUnixDateTime(this.baseSpark.startTime * 1000))
    sparkInfo.setExecutorMemory(this.baseSpark.sc.getConf.get("spark.executor.memory", "1"))
    sparkInfo.setExecutorInstances(this.baseSpark.sc.getConf.get("spark.executor.instances", "1"))
    sparkInfo.setExecutorCores(this.baseSpark.sc.getConf.get("spark.executor.cores", "1"))
    sparkInfo.setDriverCores(this.baseSpark.sc.getConf.get("spark.driver.cores", "1"))
    sparkInfo.setDriverMemory(this.baseSpark.sc.getConf.get("spark.driver.memory", "1"))
    sparkInfo.setDriverMemoryOverhead(this.baseSpark.sc.getConf.get("spark.yarn.driver.memoryOverhead", "0"))
    sparkInfo.setDriverHost(this.baseSpark.sc.getConf.get("spark.driver.host", "0"))
    sparkInfo.setDriverPort(this.baseSpark.sc.getConf.get("spark.driver.port", "0"))
    sparkInfo.setExecutorMemoryOverhead(this.baseSpark.sc.getConf.get("spark.yarn.executor.memoryOverhead", "0"))
    sparkInfo.setTopics(PropUtils.getString(GlobalConstants.PropKeys.KAFKA_TOPICS, ""))
    sparkInfo.setBrokers(PropUtils.getString(GlobalConstants.PropKeys.KAFKA_BROKERS_URL, GlobalConstants.DefaultVals.kafkaBrokers))
    sparkInfo.setGroupId(PropUtils.getString(GlobalConstants.PropKeys.KAFKA_GROUP_ID, this.baseSpark.appName))
    sparkInfo.setBatchDuration(this.baseSpark.batchDuration + "")
    sparkInfo.computeCpuMemory()

    JSON.toJSONString(sparkInfo, SerializerFeature.NotWriteRootClassName)
  }


  /**
    * 获取spark任务的webUI地址信息
    *
    * @return
    */
  private def ui: String = {
    val line = new StringBuilder()
    this.baseSpark.webUI.split(",").foreach(url => {
      line.append(StringsUtils.hrefTag(url) + StringsUtils.brTag(""))
    })

    line.toString()
  }

}
