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
 * Fire API 元数据集中注册表。
 * <p>
 * API 行为变更时：在同一次提交中更新对应实现，并在本注册表追加 {@link ApiChange}。
 * 运行时埋点仅调用 {@code LineageManager.addApiLineage(name)}，由本表补齐 module / sinceVersion。
 * </p>
 *
 * @author ChengLong
 * @since 3.0.0
 */
object ApiMetaRegistry {

  val MODULE_JDBC = "JDBC"
  val MODULE_STREAMING = "Streaming"
  val MODULE_UNKNOWN = "UNKNOWN"

  /** 未知 API 时的兜底元数据 */
  def unknown(name: String): ApiMeta = ApiMeta(name, MODULE_UNKNOWN, "", Set.empty)

  private lazy val metas: Map[String, ApiMeta] = Map(
    "jdbcUpdateBatch" -> ApiMeta(
      name = "jdbcUpdateBatch",
      module = MODULE_JDBC,
      sinceVersion = "2.3.3",
      engines = Set("spark", "flink"),
      changes = Seq(
        ApiChange("3.0.0", "2025-01-01", "fire-1286: partition 共用连接，中间批 commit=false"),
        ApiChange("3.0.0", "2025-01-01", "fire-1309: 恢复中间批默认 commit=true，避免 batchSize 整数倍静默丢数")
      )
    ),
    "jdbcBatchUpdate" -> ApiMeta(
      name = "jdbcBatchUpdate",
      module = MODULE_JDBC,
      sinceVersion = "2.0.0",
      engines = Set("spark", "flink"),
      changes = Seq(
        ApiChange("2.3.3", "2022-01-01", "deprecated，请使用 jdbcUpdateBatch")
      )
    ),
    "jdbcUpdateBatchAsync" -> ApiMeta(
      name = "jdbcUpdateBatchAsync",
      module = MODULE_JDBC,
      sinceVersion = "3.0.0",
      engines = Set("spark")
    ),
    "createRandomLongStream" -> ApiMeta(
      name = "createRandomLongStream",
      module = MODULE_STREAMING,
      sinceVersion = "2.0.0",
      engines = Set("spark", "flink")
    ),
    "foreachRDDAtLeastOnce" -> ApiMeta(
      name = "foreachRDDAtLeastOnce",
      module = MODULE_STREAMING,
      sinceVersion = "2.0.0",
      engines = Set("spark")
    ),
    "foreachPartitionAsync" -> ApiMeta(
      name = "foreachPartitionAsync",
      module = MODULE_STREAMING,
      sinceVersion = "2.0.0",
      engines = Set("spark", "flink")
    )
  )

  def get(name: String): Option[ApiMeta] = {
    if (name == null || name.isEmpty) None else metas.get(name)
  }

  def getOrUnknown(name: String): ApiMeta = get(name).getOrElse(unknown(name))

  def moduleOf(name: String): String = getOrUnknown(name).module

  def sinceVersionOf(name: String): String = getOrUnknown(name).sinceVersion

  def contains(name: String): Boolean = metas.contains(name)

  def all: Iterable[ApiMeta] = metas.values
}
