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

package com.zto.fire.spark.sync

import com.zto.fire.common.bean.lineage.{Lineage, LineageCollectData}
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.lineage.{LineageManager, SQLLineageManager}
import com.zto.fire.common.util.ReflectionUtils
import com.zto.fire.core.sync.LineageAccumulatorManager
import com.zto.fire.predef._
import com.zto.fire.spark.acc.AccumulatorManager
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.sql.SparkSqlParser
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * 用于将各个executor端数据收集到driver端
 *
 * @author ChengLong 2022-08-24 14:31:08
 * @since 2.3.2
 */
object SparkLineageAccumulatorManager extends LineageAccumulatorManager {

  val SPARK_LISTENER_CLASS = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"


  /**
   * 将完整血缘采集载荷放到累加器中（datasource + apis）
   */
  override def add(collectData: LineageCollectData): Unit = {
    AccumulatorManager.addLineage(collectData)
  }

  /**
   * 累加Long类型数据
   */
  override def add(value: Long): Unit = AccumulatorManager.addCounter(value)

  /**
   * 获取收集到的血缘消息（对外结构兼容：仅新增 apis）
   */
  override def getValue: Lineage = {
    val collect = AccumulatorManager.getLineageCollect
    // 再合并 driver 本地尚未上报的 API / datasource
    LineageManager.mergeLineageCollectData(collect, LineageManager.getLineageCollectData)
    new Lineage(collect.getDatasource, SQLLineageManager.getSQLLineage, collect.getApis)
  }

  /**
   * 开源版本暂时无法监听SparkListenerSQLExecutionEnd
   *
   * @param event
   */
  def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    if (!FireFrameworkConf.lineageEnable || !FireSparkConf.sparkLineageListenerEnable) {
      logInfo("已手动关闭spark血缘采集，如需开启请检查配置")
    }

    tryWithLog {
      val clazz = Class.forName(this.SPARK_LISTENER_CLASS)
      if (ReflectionUtils.containsField(clazz, "qe")) {
        val queryExecution = ReflectionUtils.getFieldValue(event, "qe").asInstanceOf[QueryExecution]

        if (Option(queryExecution)
          .flatMap(qe => Option(qe.sparkPlan))
          .forall(_.toString == "LocalTableScan <empty>\n")) {
          logInfo("LocalTableScan onExecutionEnd skipping")
          return
        }

        SparkSqlParser.sqlParserWithExecution(queryExecution)
      } else {
        logWarning("当前spark版本不支持血缘采集")
      }
    }(logger, "", "spark监听血缘信息解析失败", isThrow = false, hook = false)

  }
}
