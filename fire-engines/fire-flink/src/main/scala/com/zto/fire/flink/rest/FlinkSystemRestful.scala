/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.flink.rest

import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.enu.{Datasource, ErrorCode, RequestMethod}
import com.zto.fire.common.util._
import com.zto.fire.core.rest.{RestCase, RestServerManager, SystemRestful}
import com.zto.fire.flink.BaseFlink
import com.zto.fire.common.lineage._
import com.zto.fire.flink.bean.{CheckpointParams, DistributeBean}
import com.zto.fire.flink.enu.DistributeModule
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator
import spark._

/**
 * 系统预定义的restful服务，为Flink计算引擎提供接口服务
 *
 * @author ChengLong 2020年4月2日 13:50:01
 */
private[fire] class FlinkSystemRestful(var baseFlink: BaseFlink, val restfulRegister: RestServerManager) extends SystemRestful(baseFlink) {
  private var distributeJson = ""

  /**
   * 注册Flink引擎restful接口
   */
  override protected def register: Unit = {
    this.restfulRegister
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/kill", kill))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/datasource", datasource))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/lineage", lineage))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/checkpoint", checkpoint))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/distributeSync", distributeSync))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/setConf", setConf))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/arthas", arthas))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/exception", exception))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/collectLineage", collectLineage))
  }

  /**
   * 设置baseFlink实例
   */
  private[fire] def setBaseFlink(baseFlink: BaseFlink): Unit = this.baseFlink = baseFlink

  /**
   * 启用Arthas进行性能诊断
   *
   */
  @Rest("/system/arthas")
  override def arthas(request: Request, response: Response): AnyRef = {
    logInfo(s"Ip address ${request.ip()} request /system/arthas")
    val retVal = super.arthas(request, response)
    val json = request.body()
    if (JSONUtils.getValue[Boolean](json, "distribute", false)) {
      this.distributeJson = JSONUtils.toJSONString(new DistributeBean(DistributeModule.ARTHAS, request.body))
      logInfo("开始分布式分发：" + this.distributeJson)
    }
    retVal
  }

  /**
   * 用于引擎内部分布式采集血缘信息
   */
  @Rest("/system/collectLineage")
  def collectLineage(request: Request, response: Response): AnyRef = {
    val json = request.body

    try {
      logDebug(s"内部请求分布式更新血缘信息，ip：${request.ip()}")
      LineageManager.printLog(s"请求fire更新血缘信息：$json")
      if (noEmpty(json)) {
        val lineageMap = JSONUtils.parseObject[JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]](json)
        if (ValueUtils.noEmpty(lineageMap)) {
          FlinkLineageAccumulatorManager.add(lineageMap)
        }
      }
      ResultMsg.buildSuccess("血缘信息已更新", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        logError(s"[collectLineage] 设置血缘信息失败：json=$json", e)
        ResultMsg.buildError("设置血缘信息失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 用于引擎内部分布式同步信息
   */
  @Rest("/system/distributeSync")
  def distributeSync(request: Request, response: Response): AnyRef = {
    logDebug(s"内部请求分布式更新信息，ip：${request.ip()}")
    this.distributeJson
  }

  /**
   * 用于更新配置信息
   */
  @Rest("/system/setConf")
  def setConf(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      logInfo(s"Ip address ${request.ip()} request /system/setConf")
      logInfo(s"请求fire更新配置信息：$json")
      val confMap = JSONUtils.parseObject[JHashMap[String, String]](json)
      if (ValueUtils.noEmpty(confMap)) {
        PropUtils.setProperties(confMap)
        this.distributeJson = JSONUtils.toJSONString(new DistributeBean(DistributeModule.CONF, json))
      }
      ResultMsg.buildSuccess("配置信息已更新", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        logError(s"[setConf] 设置配置信息失败：json=$json", e)
        ResultMsg.buildError("设置配置信息失败", ErrorCode.ERROR)
      }
    }
  }


  /**
   * 用于运行时热修改checkpoint
   */
  @Rest("/system/checkpoint")
  def checkpoint(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      logInfo(s"Ip address ${request.ip()} request /system/checkpoint")
      val checkpointParams = JSONUtils.parseObject[CheckpointParams](json)

      val clazz = classOf[CheckpointCoordinator]
      // 获取静态方法
      val getInstance = ReflectionUtils.getMethodByName(clazz, "getInstance")
      if (getInstance != null) {
        // 获取CheckpointCoordinator单例对象
        val coordinator = getInstance.invoke(null)
        if (coordinator != null) {
          val target = coordinator.asInstanceOf[CheckpointCoordinator]
          // 重新设置checkpoint的频率
          if (checkpointParams.getInterval != null) ReflectionUtils.getMethodByName(clazz, "setBaseInterval").invoke(target, checkpointParams.getInterval)
          // 重新设置checkpoint的超时时间
          if (checkpointParams.getTimeout != null) ReflectionUtils.getMethodByName(clazz, "setCheckpointTimeout").invoke(target, checkpointParams.getTimeout)
          // 重新设置两次相邻checkpoint的最短时间间隔
          if (checkpointParams.getMinPauseBetween != null) ReflectionUtils.getMethodByName(clazz, "setMinPauseBetweenCheckpoints").invoke(target, checkpointParams.getMinPauseBetween)
          // 重新调度checkpoint
          target.startCheckpointScheduler()
        }
      }

      logInfo(s"[checkpoint] 执行checkpoint热修改成功：interval=${checkpointParams.getInterval} timeout=${checkpointParams.getTimeout} minPauseBetween=${checkpointParams.getMinPauseBetween} json=$json")
      ResultMsg.buildSuccess(s"执行checkpoint热修改成功：interval=${checkpointParams.getInterval} timeout=${checkpointParams.getTimeout} minPauseBetween=${checkpointParams.getMinPauseBetween}", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        logError(s"[checkpoint] 执行checkpoint热修改失败：json=$json", e)
        ResultMsg.buildError("执行checkpoint热修改失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * kill 当前 Flink 任务
   */
  @Rest("/system/kill")
  def kill(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      logInfo(s"Ip address ${request.ip()} request /system/kill")
      // 参数校验与参数获取
      this.baseFlink.shutdown()
      logInfo(s"[kill] kill任务成功：json=$json")
      ResultMsg.buildSuccess("任务停止成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        logError(s"[kill] 执行kill任务失败：json=$json", e)
        ResultMsg.buildError("执行kill任务失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 用于执行sql语句
   */
  @Rest(value = "/system/sql", method = "post")
  def sql(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      logInfo(s"Ip address ${request.ip()} request /system/sql")
      // 参数校验与参数获取
      val sql = JSONUtils.getValue(json, "sql", "")

      // sql合法性检查
      if (StringUtils.isBlank(sql) || !sql.toLowerCase.trim.startsWith("select ")) {
        logWarning(s"[sql] sql不合法，在线调试功能只支持查询操作：json=$json")
        return ResultMsg.buildError(s"sql不合法，在线调试功能只支持查询操作", ErrorCode.ERROR)
      }

      if (this.baseFlink == null) {
        logWarning(s"[sql] 系统正在初始化，请稍后再试：json=$json")
        return "系统正在初始化，请稍后再试"
      }

      ""
    } catch {
      case e: Exception => {
        logError(s"[sql] 执行用户sql失败：json=$json", e)
        ResultMsg.buildError("执行用户sql失败，异常堆栈：" + ExceptionBus.stackTrace(e), ErrorCode.ERROR)
      }
    }
  }
}
