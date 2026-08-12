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

import java.util.{List => JList}
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
 * @param clazz        API 所在类全限定名
 * @param api          API 名称（与埋点名一致）
 * @param module       模块：JDBC / Streaming / HBase 等
 * @param sinceVersion 首次引入的 fire 版本
 * @param engines      适用引擎：spark / flink
 * @param changes      行为变更历史
 */
case class ApiMeta(clazz: String,
                   api: String,
                   module: String,
                   sinceVersion: String,
                   engines: Set[String] = Set("spark", "flink"),
                   changes: Seq[ApiChange] = Nil) {
  def identityKey: String = {
    val c = if (clazz == null) "" else clazz.trim
    val a = if (api == null) "" else api.trim
    c + "#" + a
  }
}

/**
 * Fire API 元数据集中注册表 api-lineage.yaml
 *
 * @author ChengLong
 * @since 3.0.0
 */
object ApiMetaRegistry extends Logging {
  val JDBC = "JDBC"
  val HBASE = "HBase"
  val STREAMING = "Streaming"
  val UNKNOWN = "UNKNOWN"

  // api血缘变更日志记录文件
  private val RESOURCE_NAME = "api-lineage.yaml"

  private case class LoadedYaml(metas: Map[String, ApiMeta], byApi: Map[String, ApiMeta], holders: Seq[String])

  def unknown(api: String): ApiMeta = ApiMeta("", Option(api).getOrElse(""), UNKNOWN, "", Set.empty)

  def unknown(clazz: String, api: String): ApiMeta =
    ApiMeta(Option(clazz).getOrElse(""), Option(api).getOrElse(""), UNKNOWN, "", Set.empty)

  private lazy val loaded: LoadedYaml = this.loadFromYaml()

  private def metas: Map[String, ApiMeta] = loaded.metas

  private def byApi: Map[String, ApiMeta] = loaded.byApi

  /**
   * 声明了 @API 的扩展类列表（来自 yaml holders）
   */
  def holders: Seq[String] = loaded.holders

  /**
   * Java 侧读取 holders
   */
  def holdersJava: JList[String] = loaded.holders.asJava

  /**
   * 从 classpath 加载 api-lineage.yaml
   */
  private def loadFromYaml(): LoadedYaml = {
    val classLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    val stream = classLoader.getResourceAsStream(RESOURCE_NAME)
    if (stream == null) {
      logError(s"未找到 classpath 资源 $RESOURCE_NAME，API 元数据注册表为空（请确认 fire-core 已打入运行包）")
      return LoadedYaml(Map.empty, Map.empty, Nil)
    }

    try {
      val mapper = new ObjectMapper(new YAMLFactory)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
        .setVisibility(PropertyAccessor.ALL, Visibility.ANY)

      // 加载yaml配置文件并映射成ApiLineageConfig类型，便于后续使用
      val config = mapper.readValue(stream, classOf[ApiLineageConfig])
      if (config == null) {
        logWarning(s"$RESOURCE_NAME 解析结果为空")
        return LoadedYaml(Map.empty, Map.empty, Nil)
      }

      val holders = parseHolders(config)
      if (holders.isEmpty) {
        logWarning(s"$RESOURCE_NAME 未配置 holders，API 血缘织入将无法发现目标类")
      } else {
        logInfo(s"已从 $RESOURCE_NAME 加载 holders ${holders.size} 条")
      }

      if (config.getApis == null || config.getApis.isEmpty) {
        logWarning(s"$RESOURCE_NAME 未配置任何 API 元数据")
        return LoadedYaml(Map.empty, Map.empty, holders)
      }

      // 将加载到的配置文件映射成 map：主键 class#api，并保留 api → 任一条元数据兜底
      val map = mutable.LinkedHashMap[String, ApiMeta]()
      val apiMap = mutable.LinkedHashMap[String, ApiMeta]()
      config.getApis.asScala.foreach(item => {
        if (item != null && StringUtils.isNotBlank(item.getApi)) {
          val meta = toApiMeta(item)
          val key = meta.identityKey
          if (map.contains(key)) {
            logWarning(s"$RESOURCE_NAME 存在重复 API，后者覆盖前者：class=${meta.clazz} api=${meta.api}")
          }
          map.put(key, meta)
          apiMap.put(meta.api, meta)
        }
      })
      logInfo(s"已从 $RESOURCE_NAME 加载 API 元数据 ${map.size} 条")

      LoadedYaml(map.toMap, apiMap.toMap, holders)
    } catch {
      case e: Throwable =>
        logError(s"解析 $RESOURCE_NAME 失败，API 元数据注册表为空", e)
        LoadedYaml(Map.empty, Map.empty, Nil)
    } finally {
      try stream.close() catch { case _: Throwable => }
    }
  }

  private def parseHolders(config: ApiLineageConfig): Seq[String] = {
    if (config.getHolders == null || config.getHolders.isEmpty) {
      Nil
    } else {
      config.getHolders.asScala
        .filter(_ != null)
        .map(_.trim)
        .filter(StringUtils.isNotBlank)
        .toList
        .distinct
    }
  }

  /**
   * 将配置文件中的各个字段映射为ApiMeta类型
   */
  private def toApiMeta(item: ApiLineageConfig.ApiMetaItem): ApiMeta = {
    val clazz = if (item.getClazz == null) "" else item.getClazz.trim
    val api = item.getApi.trim
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

    ApiMeta(clazz, api, module, since, engines, changes)
  }

  /**
   * 按 class#api 精确查找
   */
  def get(clazz: String, api: String): Option[ApiMeta] = {
    if (api == null || api.isEmpty) None
    else {
      val key = (if (clazz == null) "" else clazz.trim) + "#" + api.trim
      metas.get(key).orElse(byApi.get(api.trim))
    }
  }

  /**
   * 仅按 api 名称查找（多 class 同名时返回登记表中的一条）
   */
  def get(api: String): Option[ApiMeta] = {
    if (api == null || api.isEmpty) None else byApi.get(api.trim)
  }

  def getOrUnknown(api: String): ApiMeta = get(api).getOrElse(unknown(api))

  def getOrUnknown(clazz: String, api: String): ApiMeta =
    get(clazz, api).getOrElse(unknown(clazz, api))

  def moduleOf(api: String): String = getOrUnknown(api).module

  def sinceVersionOf(api: String): String = getOrUnknown(api).sinceVersion

  def contains(api: String): Boolean = byApi.contains(api)

  def contains(clazz: String, api: String): Boolean = get(clazz, api).isDefined

  def all: Iterable[ApiMeta] = metas.values
}
