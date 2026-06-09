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

package com.zto.fire.core.sync

import com.zto.fire.common.bean.standard.StandardResult
import com.zto.fire.common.util.{FireUtils, Logging}
import org.apache.log4j.Logger

import java.lang.reflect.Method

/**
 * TraceStandard 采集入口，按当前引擎将数据转交给 Spark/Flink 各自的 StandardAccumulatorManager 实现。
 *
 * @author ChengLong
 * @since 3.0.0
 */
object StandardAccumulatorManagerHelper extends Logging {
  private[this] lazy val SPARK_MANAGER = "com.zto.fire.spark.sync.SparkStandardAccumulatorManager$"
  private[this] lazy val FLINK_MANAGER = "com.zto.fire.flink.sync.FlinkStandardAccumulatorManager$"
  @volatile private[this] var collector: StandardCollector = _

  /**
   * 添加单条代码标准化分析结果，不直接依赖 Spark/Flink 模块，避免 core 引入具体引擎依赖。
   */
  def add(result: StandardResult): Unit = {
    if (result == null) return

    try {
      val standardCollector = this.getCollector
      if (standardCollector == null) return

      standardCollector.add(result)
    } catch {
      case e: Throwable => logger.warn("Trace standard 采集结果添加到累加器失败", e)
    }
  }

  private def getCollector: StandardCollector = {
    var current = this.collector
    if (current != null) return current

    this.synchronized {
      current = this.collector
      if (current != null) return current

      val managerClass = if (FireUtils.isSparkEngine) {
        SPARK_MANAGER
      } else if (FireUtils.isFlinkEngine) {
        FLINK_MANAGER
      } else {
        ""
      }
      if (managerClass.isEmpty) return null

      val manager = this.loadScalaObject(managerClass)
      val addMethod = manager.getClass.getMethod("add", classOf[StandardResult])
      this.collector = StandardCollector(manager, addMethod)
      this.collector
    }
  }

  private def loadScalaObject(className: String): AnyRef = {
    val clazz = Class.forName(className)
    val module = clazz.getField("MODULE$")
    module.get(null)
  }

  private case class StandardCollector(manager: AnyRef, addMethod: Method) {
    def add(result: StandardResult): Unit = this.addMethod.invoke(this.manager, result)
  }
}
