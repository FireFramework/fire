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

package com.zto.fire.spark.util

import com.zto.fire._
import com.zto.fire.common.bean.TableIdentifier
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * TiSpark工具类，用于执行计划解析
 *
 * @author ChengLong
 * @Date 2024/4/13 08:43
 * @version 2.4.5
 */
object TiSparkUtils {
  private lazy val tidbTableMap = new JConcurrentHashMap[String, Boolean]()
  private lazy val tableReference = "TiTableReference(.*[1-9])".r

  /**
   * 判断给定的表是否为TIDB表
   */
  def isTiDBTable(tableIdentifier: TableIdentifier): Boolean = {
    this.tidbTableMap.mergeGet(tableIdentifier.identifier) {
      try {
        val tisparkContextClass = Class.forName("org.apache.spark.sql.TiContext")
        val constructor = tisparkContextClass.getConstructor(classOf[SparkSession])
        val tiContext = constructor.newInstance(SparkSingletonFactory.getSparkSession)
        val metaMethod = tisparkContextClass.getMethod("meta")
        val meta = metaMethod.invoke(tiContext)
        val method = meta.getClass.getMethod("getTableFromCache", classOf[String], classOf[String])
        method.invoke(meta, tableIdentifier.database, tableIdentifier.table).asInstanceOf[Option[_]].isDefined
      }catch {
        case e: Throwable => false
      }
    }
  }

  /**
   * 从逻辑执行计划中解析TiSpark所使用的tidb表
   */
  def parseTableIdentifier(logicalRelation: LogicalRelation): Option[TableIdentifier] = {
    // 如果是TiSpark，则使用反射进行血缘解析，好处是可以避免引入tispark的依赖
    try {
      val relationMethod = logicalRelation.getClass.getDeclaredMethod("relation")
      val relation = relationMethod.invoke(logicalRelation)
      val tableRefMethod = relation.getClass.getDeclaredMethod("tableRef")
      val tiTableRef = tableRefMethod.invoke(relation)
      val dbNameMethod = tiTableRef.getClass.getDeclaredMethod("databaseName")
      val dbName = dbNameMethod.invoke(tiTableRef)
      val tableMethod = tiTableRef.getClass.getDeclaredMethod("tableName")
      val tableName = tableMethod.invoke(tiTableRef)

      Some(TableIdentifier(tableName.toString, dbName.toString))
    } catch {
      case e: Throwable => None
    }
  }

  /**
   * 通过执行计划解析TiSpark所使用的tidb表
   */
  def parseTableIdentifier(logicalPlan: LogicalPlan): Option[TableIdentifier] = {
    val plan = logicalPlan.toString()
    if (!plan.contains("TiDBRelation")) return None

    val reference = tableReference.findFirstIn(plan)
    if (reference.isEmpty) return None

    val fields = reference.get.replace("TiTableReference(", "").split(",")
    if (fields.isEmpty || fields.length < 2) return None

    Some(TableIdentifier(fields(1), fields(0)))
  }
}
