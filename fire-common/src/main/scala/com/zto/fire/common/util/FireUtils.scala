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

package com.zto.fire.common.util

import com.zto.fire.common.anno.Internal
import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import com.zto.fire.common.enu.{JobType, RunMode}
import com.zto.fire.predef._

/**
 * fire框架通用的工具方法
 * 注：该工具类中不可包含Spark或Flink的依赖
 *
 * @author ChengLong
 * @since 1.0.0
 * @create: 2020-05-17 10:17
 */
private[fire] object FireUtils extends Serializable with Logging {
  private[fire] var isSplash = false
  private[fire] var _jobType: JobType = JobType.UNDEFINED
  private[fire] val _launchTime = System.currentTimeMillis()
  private[this] lazy val sparkUtils = "com.zto.fire.spark.util.SparkUtils"
  private[this] lazy val flinkUtils = "com.zto.fire.flink.util.FlinkUtils"

  /**
   * 获取任务启动时间
   */
  def launchTime: Long = this._launchTime

  /**
   * 任务运行时间
   */
  def uptime: Long = System.currentTimeMillis() - this.launchTime

  /**
   * 判断是否为spark引擎
   */
  def isSparkEngine: Boolean = "spark".equals(this.engine)

  /**
   * 判断是否为flink引擎
   */
  def isFlinkEngine: Boolean = "flink".equals(this.engine)

  /**
   * 用于判断任务类型是否为流式计算任务
   */
  def isStreamingJob: Boolean = !this.isBatchJob

  /**
   * 用于判断任务类型是否为批处理任务
   * @return
   */
  def isBatchJob: Boolean = {
    this.jobType == JobType.SPARK_CORE || this.jobType == JobType.SPARK_SQL || this.jobType == JobType.FLINK_BATCH
  }

  /**
   * 获取当前实时任务所使用的计算引擎
   * @return
   * spark / flink
   */
  def engine: String = PropUtils.engine

  /**
   * 当前任务的引擎类型
   */
  def jobType: JobType = this._jobType

  /**
   * 获取fire版本号
   */
  def fireVersion: String = FireFrameworkConf.fireVersion

  /**
   * 获取当前执行引擎的版本号
   * @return
   * spark-version / flink-version
   */
  def engineVersion: String = invokeEngineUtils[String]("getVersion", null)

  /**
   * 获取当前执行引擎运行时的appId
   */
  def applicationId: String = invokeEngineUtils[String]("getApplicationId")

  /**
   * 任务发布类型：yarn-client/yarn-cluster/run-application
   */
  def deployMode: String = invokeEngineUtils[String]("deployMode")

  /**
   * SQL血缘解析
   */
  def sqlParser(sql: String): Unit = invokeEngineUtils[Unit]("sqlParser", Array[Class[_]](classOf[String]), Array[Object](sql))

  /**
   * 用于判断引擎是否已完成上下文的初始化
   * 1. Spark：SparkContext
   * 2. Flink: ExecutionEnv
   */
  def isEngineUp: Boolean = invokeEngineUtils[Boolean]("isEngineUp")

  /**
   * 用于判断引擎是否已销毁上下文
   * 1. Spark：SparkContext
   * 2. Flink: ExecutionEnv
   */
  def isEngineDown: Boolean = !invokeEngineUtils[Boolean]("isEngineDown")

  /**
   * 反射调用不同引擎上层的工具方法
   * @param methodName
   * spark或flink工具类的方法名
   * @return
   */
  @Internal
  private[this] def invokeEngineUtils[T](methodName: JString, parameterTypes: Array[Class[_]] = Array[Class[_]](), args: Array[Object] = Array[Object]()): T = {
    tryWithReturn {
      if (this.isSparkEngine) {
        val getVersionMethod = ReflectionUtils.getMethodByParameter(Class.forName(sparkUtils), methodName, parameterTypes: _*)
        getVersionMethod.invoke(null, args: _*).asInstanceOf[T]
      } else {
        val getVersionMethod = ReflectionUtils.getMethodByParameter(Class.forName(flinkUtils), methodName, parameterTypes: _*)
        getVersionMethod.invoke(null, args: _*).asInstanceOf[T]
      }
    } (this.logger, catchLog = s"反射调用工具类方法[$methodName]失败")
  }

  /**
   * 当前任务实例的主类名：packageName+className
   */
  def mainClass: String = FireFrameworkConf.driverClassName

  /**
   * 退出jvm
   * @param status
   * 状态码
   */
  def exit(status: Int): Unit = System.exit(status)

  /**
   * 正常退出jvm
   */
  def exitNormal: Unit = this.exit(0)

  /**
   * 非正常退出jvm
   */
  def exitError: Unit = this.exit(-1)

  /**
   * 用于获取任务在实时平台中的唯一id标识
   */
  def platformAppId: String = FireFrameworkConf.configCenterAppId

  /**
   * 获取配置的任务运行模式
   */
  def runMode: RunMode = RunMode.parse(FireFrameworkConf.fireJobRunMode)

  /**
   * 用于判断是否以local模式提交执行
   */
  def isLocalRunMode: Boolean = {
    if (RunMode.LOCAL == FireUtils.runMode) {
      true
    } else if (RunMode.AUTO == this.runMode &&  OSUtils.isLocal) {
      true
    } else false
  }

  /**
   * 用于在fire框架启动时展示信息
   */
  private[fire] def splash: Unit = {
    if (!isSplash) {
      val engineVersion = if (this.isSparkEngine) s"spark version:${this.engineVersion}" else s"flink version:${this.engineVersion}"
      val info =
        """
          |       ___                       ___           ___
          |     /\  \          ___        /\  \         /\  \
          |    /::\  \        /\  \      /::\  \       /::\  \
          |   /:/\:\  \       \:\  \    /:/\:\  \     /:/\:\  \
          |  /::\~\:\  \      /::\__\  /::\~\:\  \   /::\~\:\  \
          | /:/\:\ \:\__\  __/:/\/__/ /:/\:\ \:\__\ /:/\:\ \:\__\
          | \/__\:\ \/__/ /\/:/  /    \/_|::\/:/  / \:\~\:\ \/__/
          |      \:\__\   \::/__/        |:|::/  /   \:\ \:\__\
          |       \/__/    \:\__\        |:|\/__/     \:\ \/__/
          |                 \/__/        |:|  |        \:\__\
          |                               \|__|         \/__/     version
          |
          |""".stripMargin.replace("version", s"fire version:${FirePS1Conf.PINK + this.fireVersion + FirePS1Conf.GREEN} $engineVersion")

      this.logger.warn(FirePS1Conf.GREEN + info + FirePS1Conf.DEFAULT)
      this.isSplash = true
    }
  }
}
