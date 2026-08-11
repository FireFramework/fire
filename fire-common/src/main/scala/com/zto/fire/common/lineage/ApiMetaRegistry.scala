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

package com.zto.fire.common.lineage

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.zto.fire.common.bean.lineage.ApiLineageConfig
import com.zto.fire.common.util.Logging
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Fire API 变更记录（仅维护在注册表中，不随运行时血缘消息下发完整 changelog）
 *
 * @param version  变更对应的 fire 版本或需求号，如 3.0.0 / fire-1309
 * @param date     变更日期，建议 yyyy-MM-dd
 * @param summary  变更说明摘要
 */
case class ApiChange(version: String, date: String, summary: String)

/**
 * Fire API 静态元数据
 *
 * @param name         API 名称（与埋点名一致）
 * @param module       模块：JDBC / Streaming / HBase 等
 * @param sinceVersion 首次引入的 fire 版本
 * @param engines      适用引擎：spark / flink
 * @param changes      行为变更历史
 */
case class ApiMeta(name: String,
                   module: String,
                   sinceVersion: String,
                   engines: Set[String] = Set("spark", "flink"),
                   changes: Seq[ApiChange] = Nil)

/**
 * Fire API 元数据集中注册表 api-lineage.yaml
 *
 * @author ChengLong
 * @since 3.0.0
 */
object ApiMetaRegistry extends Logging {
  val JDBC = "JDBC"
  val STREAMING = "Streaming"
  val UNKNOWN = "UNKNOWN"

  // api血缘变更日志记录文件
  private val RESOURCE_NAME = "api-lineage.yaml"

  def unknown(name: String): ApiMeta = ApiMeta(name, UNKNOWN, "", Set.empty)

  private lazy val metas: Map[String, ApiMeta] = this.loadFromYaml()

  /**
   * 从 classpath 加载 api-lineage.yaml
   */
  private def loadFromYaml(): Map[String, ApiMeta] = {
    val classLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    val stream = classLoader.getResourceAsStream(RESOURCE_NAME)
    if (stream == null) {
      logError(s"未找到 classpath 资源 $RESOURCE_NAME，API 元数据注册表为空（请确认 fire-core 已打入运行包）")
      return Map.empty
    }

    try {
      val mapper = new ObjectMapper(new YAMLFactory)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
        .setVisibility(PropertyAccessor.ALL, Visibility.ANY)

      // 加载yaml配置文件并映射成ApiLineageConfig类型，便于后续使用
      val config = mapper.readValue(stream, classOf[ApiLineageConfig])
      if (config == null || config.getApis == null || config.getApis.isEmpty) {
        logWarning(s"$RESOURCE_NAME 未配置任何 API 元数据")
        return Map.empty
      }

      // 将加载到的配置文件映射成map结构
      val map = mutable.LinkedHashMap[String, ApiMeta]()
      config.getApis.asScala.foreach(item => {
        if (item != null && StringUtils.isNotBlank(item.getName)) {
          val name = item.getName.trim
          if (map.contains(name)) {
            logWarning(s"$RESOURCE_NAME 存在重复 API 名称，后者覆盖前者：name=$name")
          }

          // 将api的名称与详细的变更记录映射成map
          map.put(name, toApiMeta(item))
        }
      })
      logInfo(s"已从 $RESOURCE_NAME 加载 API 元数据 ${map.size} 条")

      map.toMap
    } catch {
      case e: Throwable =>
        logError(s"解析 $RESOURCE_NAME 失败，API 元数据注册表为空", e)
        Map.empty
    } finally {
      try stream.close() catch { case _: Throwable => }
    }
  }

  /**
   * 将配置文件中的各个字段映射为ApiMeta类型
   */
  private def toApiMeta(item: ApiLineageConfig.ApiMetaItem): ApiMeta = {
    val module = if (StringUtils.isBlank(item.getModule)) UNKNOWN else item.getModule.trim
    val since = if (item.getSinceVersion == null) "" else item.getSinceVersion.trim
    val engines = if (item.getEngines == null || item.getEngines.isEmpty) {
      Set("spark", "flink")
    } else {
      item.getEngines.asScala.map(_.trim).filter(StringUtils.isNotBlank).toSet
    }

    val changes =
      if (item.getChanges == null || item.getChanges.isEmpty) Nil
      else item.getChanges.asScala.filter(_ != null).map(c =>
        ApiChange(
          Option(c.getVersion).getOrElse(""),
          Option(c.getDate).getOrElse(""),
          Option(c.getSummary).getOrElse("")
        )
      ).toList

    ApiMeta(item.getName.trim, module, since, engines, changes)
  }

  def get(name: String): Option[ApiMeta] = {
    if (name == null || name.isEmpty) None else metas.get(name)
  }

  def getOrUnknown(name: String): ApiMeta = get(name).getOrElse(unknown(name))

  def moduleOf(name: String): String = getOrUnknown(name).module

  def sinceVersionOf(name: String): String = getOrUnknown(name).sinceVersion

  def contains(name: String): Boolean = metas.contains(name)

  def all: Iterable[ApiMeta] = metas.values
}
