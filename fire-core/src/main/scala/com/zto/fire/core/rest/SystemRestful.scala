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

package com.zto.fire.core.rest

import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.analysis.ExceptionMsg
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.ErrorCode
import com.zto.fire.common.util._
import com.zto.fire.core.BaseFire
import com.zto.fire.core.bean.ArthasParam
import com.zto.fire.core.plugin.ArthasDynamicLauncher
import com.zto.fire.core.sync.SyncEngineConfHelper
import com.zto.fire.predef.noEmpty
import org.apache.commons.httpclient.Header
import spark.{Request, Response}

import scala.collection.JavaConversions

/**
 * 系统预定义的restful服务抽象
 *
 * @author ChengLong 2020年4月2日 13:58:08
 */
protected[fire] abstract class SystemRestful(engine: BaseFire) extends Logging {
  this.register

  /**
   * 注册接口
   */
  protected def register: Unit

  /**
   * 获取当前任务所使用到的数据源信息
   *
   * @return
   * 数据源列表
   */
  @Rest("/system/datasource")
  protected def datasource(request: Request, response: Response): AnyRef = {
    this.lineage(request, response)
  }

  /**
   * 获取当前任务所使用到的实时血缘信息
   */
  @Rest("/system/lineage")
  protected def lineage(request: Request, response: Response): AnyRef = {
    try {
      logInfo(s"Ip address ${request.ip()} request /system/lineage")
      val lineage = JSONUtils.toJSONString(SyncEngineConfHelper.syncLineage)
      logInfo(s"[lineage] 获取数据源列表成功：lineage=$lineage")
      ResultMsg.buildSuccess(lineage, "获取数据源列表成功")
    } catch {
      case e: Exception => {
        logError(s"[lineage] 获取实时血缘信息失败", e)
        ResultMsg.buildError("获取实时血缘信息失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 启用Arthas进行性能诊断
   *
   */
  @Rest("/system/arthas")
  protected def arthas(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      logInfo(s"Ip address ${request.ip()} request /system/arthas")
      logInfo(s"请求执行Arthas命令：$json")
      val arthasParam = JSONUtils.parseObject[ArthasParam](json)
      ArthasDynamicLauncher.command(arthasParam)
      logInfo(s"[arthas] Arthas命令${arthasParam.getCommand}执行成功！")
      ResultMsg.buildSuccess("操作成功", "调用arthas接口成功！")
    } catch {
      case e: Exception => {
        logError(s"[arthas] 调用arthas接口失败，参数不合法，请检查", e)
        ResultMsg.buildError("调用arthas接口失败，参数不合法，请检查", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 异常信息采集接口
   */
  @Rest("/system/exception")
  protected def exception(request: Request, response: Response): AnyRef = {
    try {
      logInfo(s"Ip address ${request.ip()} request /system/exception")
      val msg = ExceptionBus.getAndClear
      val exceptions = msg._1.map(t => new ExceptionMsg(t._2, t._3))
      logDebug(s"异常诊断：本轮发送异常共计${msg._1.size}个.")
      ResultMsg.buildSuccess(JSONUtils.toJSONString(JavaConversions.seqAsJavaList(exceptions)), s"获取exception信息成功，共计：${exceptions.size}条")
    } catch {
      case e: Exception => {
        logError(s"调用exception接口失败，请检查", e)
        ResultMsg.buildError("调用exception接口失败，请检查", ErrorCode.ERROR)
      }
    }
  }
}

private[fire] object SystemRestful extends Logging {
  private var logCount = 0

  /**
   * fire框架内部接口调用工具
   *
   * @param urlSuffix
   * 接口后缀
   * @param json
   * 请求参数
   * @return
   * 接口响应结果
   */
  def restInvoke(urlSuffix: String, json: String = ""): String = {
    var response: String = ""
    if (FireFrameworkConf.restEnable && noEmpty(FireFrameworkConf.fireRestUrl, urlSuffix)) {
      val restful = FireFrameworkConf.fireRestUrl + urlSuffix
      try {
        val secret = EncryptUtils.md5Encrypt(FireFrameworkConf.dynamicKey)
        logDebug(s"secret=${secret} restServerSecret=${FireFrameworkConf.restServerSecret} driverClassName=${FireFrameworkConf.driverClassName}  date=${DateFormatUtils.formatCurrentDate}")
        response = if (noEmpty(json)) {
          HttpClientUtils.doPost(restful, json, new Header("Content-Type", "application/json"), new Header("Authorization", secret))
        } else {
          HttpClientUtils.doGet(restful, new Header("Content-Type", "application/json"), new Header("Authorization", secret))
        }
      } catch {
        case e: Exception => {
          if (this.logCount < 3) {
            logInfo(s"fire内部接口自调用失败，对任务无影响，请忽略该异常", e)
            this.logCount += 1
          }
        }
      }
    }
    response
  }
}