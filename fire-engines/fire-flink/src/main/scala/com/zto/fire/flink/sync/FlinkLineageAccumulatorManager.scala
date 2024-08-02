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

package com.zto.fire.flink.sync

import com.zto.fire._
import com.zto.fire.common.bean.lineage.Lineage
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.lineage.{DatasourceDesc, LineageManager, SQLLineageManager}
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.sync.LineageAccumulatorManager

import java.util.concurrent.atomic.AtomicLong

/**
 * 用于将各个TaskManager端数据收集到JobManager端
 *
 * @author ChengLong 2022-08-29 16:29:17
 * @since 2.3.2
 */
object FlinkLineageAccumulatorManager extends LineageAccumulatorManager {
  private lazy val lineageMap = new JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]()
  private lazy val counter = new AtomicLong()

  /**
   * 去重合并血缘信息
   */
  private def mergeLineage(lineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
    // 合并来自各个TaskManager端的血缘信息
    if (lineage.nonEmpty) merge(lineage)
    // 合并血缘管理器中的血缘信息
    if (LineageManager.getDatasourceLineage.nonEmpty) merge(LineageManager.getDatasourceLineage)

    /**
     * 合并血缘
     */
    def merge(lineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
      LineageManager.printLog(s"Flink血缘累加器merge this.lineageMap=${this.lineageMap} 待合并map：$lineage")
      lineage.foreach(each => {
        val key = if (each._1.isInstanceOf[String]) Datasource.parse(each._1.asInstanceOf[String]) else each._1
        val set = this.lineageMap.mergeGet(key)(new JHashSet[DatasourceDesc]())

        // 兼容jackson反序列化不支持复杂泛型嵌套结构，如：JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]
        if (each._2.isInstanceOf[JArrayList[_]]) {
          // local模式
          LineageManager.printLog("Flink血缘累加器：mergeLineage.local")
          val datasource = each._2.asInstanceOf[JArrayList[JMap[String, _]]]
          datasource.foreach(map => {
            if (map.containsKey("datasource")) {
              val datasource = map.getOrElse("datasource", "").toString.toUpperCase
              val datasourceClazz = Datasource.toDatasource(Datasource.parse(datasource))
              if (datasourceClazz != null) {
                val json = JSONUtils.toJSONString(map)
                val datasourceDesc = JSONUtils.parseScalaObject(json, datasourceClazz)
                LineageManager.printLog(s"Flink血缘累加器json：$json json反序列化：$datasourceDesc")
                LineageManager.mergeSet(set, datasourceDesc.asInstanceOf[DatasourceDesc])
              }
            }
          })
        } else {
          // cluster模式
          LineageManager.printLog("Flink血缘累加器：mergeLineage.cluster")
          each._2.filter(desc => desc.toString.contains("datasource")).foreach(desc => LineageManager.mergeSet(set, desc))
        }

        if (set.nonEmpty) this.lineageMap.put(key, set)
        LineageManager.printLog(s"Flink血缘累加器合并完成：${this.lineageMap}")
      })
    }
  }

  /**
   * 将血缘信息放到累加器中
   */
  override def add(lineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = this.synchronized {
    if (lineage.nonEmpty) this.mergeLineage(lineage)
  }

  /**
   * 累加Long类型数据
   */
  override def add(value: Long): Unit = this.counter.addAndGet(value)

  /**
   * 获取收集到的血缘消息
   */
  override def getValue: Lineage = {
    new Lineage(this.lineageMap, SQLLineageManager.getSQLLineage)
  }
}