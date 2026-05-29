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

import com.zto.fire.common.bean.standard.Standard
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
  def add(standard: Standard): Unit = {
    if (standard == null) return

    try {
      // 根据不同的运行时引擎类型，获取对应的累加器实例
      val standardCollector = this.getCollector
      if (standardCollector == null) return

      // 将最新的扫描数据放到累加器中，等待定时任务将检测结果发送到指定消息队列
      standardCollector.add(standard)
    } catch {
      case e: Throwable => logger.warn("Trace standard 采集结果添加到累加器失败", e)
    }
  }

  /**
   * 获取当前引擎对应的采集器，首次调用时完成反射解析，后续复用缓存的实例与方法。
   */
  private def getCollector: StandardCollector = {
    var current = this.collector
    if (current != null) return current

    this.synchronized {
      current = this.collector
      if (current != null) return current

      // 根据不同的引擎创建不同的累加器实例
      val managerClass = if (FireUtils.isSparkEngine) {
        SPARK_MANAGER
      } else if (FireUtils.isFlinkEngine) {
        FLINK_MANAGER
      } else {
        ""
      }
      if (managerClass.isEmpty) return null

      val manager = this.loadScalaObject(managerClass)
      val addMethod = manager.getClass.getMethod("add", classOf[Standard])
      // 缓存必要的反射对象，降低开销
      this.collector = StandardCollector(manager, addMethod)
      this.collector
    }
  }

  /**
   * 根据引擎反射创建对应的累加器
   */
  private def loadScalaObject(className: String): AnyRef = {
    val clazz = Class.forName(className)
    val module = clazz.getField("MODULE$")
    module.get(null)
  }

  /**
   * 缓存引擎侧 StandardAccumulatorManager 实例与 add 方法，避免每次命中都反射查找。
   */
  private case class StandardCollector(manager: AnyRef, addMethod: Method) {
    def add(standard: Standard): Unit = this.addMethod.invoke(this.manager, standard)
  }
}
