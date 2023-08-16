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

package com.zto.fire.common.lineage.parser

import com.zto.fire.predef._
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.enu.Operation
import com.zto.fire.common.lineage.{DatasourceDesc, LineageManager, SQLLineageManager}
import com.zto.fire.common.util.ReflectionUtils

import java.util.concurrent.CopyOnWriteArraySet
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * connector血缘解析管理器
 *
 * @author ChengLong 2023-08-09 10:07:45
 * @since 2.3.8
 */
private[fire] object ConnectorParserManager extends ConnectorParser {
  private lazy val packageName = "com.zto.fire.common.lineage.parser.connector"

  /**
   * 根据connector名称获取完整的包名与类型信息
   *
   * @param connector
   * connector的类型
   * @return
   * 包名+类名
   */
  private[this] def getClassName(connector: String): Option[String] = {
    if (isEmpty(connector)) return None

    Some(s"${packageName}.${connector.replaceAll("fire-", "").toLowerCase.headUpper}Connector")
  }

  /**
   * 解析指定的connector血缘
   *
   * @param tableIdentifier
   * 表的唯一标识
   * @param properties
   * connector中的options信息
   */
  override def parse(tableIdentifier: TableIdentifier, properties: mutable.Map[String, String], partitions: String): Unit = {
    val connector = properties.getOrElse("connector", "")
    val className = this.getClassName(connector)
    if (className.isDefined) {
      try {
        val method = ReflectionUtils.getMethodByName(className.get, "parse")
        if (method != null) {
          SQLLineageManager.setConnector(tableIdentifier, connector)
          method.invoke(null, tableIdentifier, properties, partitions)
          LineageManager.printLog(s"映射SQL血缘为Datasource，反射调用类：${className.get}.parse()，connector：$connector properties：$properties")
        }
      } catch {
        case e: Throwable => logWarning(s"血缘解析失败：不支持的connector类型[$connector]", e)
      }
    }
  }

  /**
   * 用于定义如何合并相同的数据源
   * 注：operation字段必须是var变量，否则合并不成功
   *
   * @param datasourceList
   * 当前数据源列表
   * @param targetDesc
   * 待合并的数据源
   */
  def merge[T <: DatasourceDesc : ClassTag](datasourceList: JSet[DatasourceDesc], targetDesc: DatasourceDesc): JHashSet[DatasourceDesc] = {
    val mergeSet = new CopyOnWriteArraySet[DatasourceDesc](datasourceList)
    mergeSet.addAll(datasourceList)

    mergeSet.foreach(sourceDesc => {
      if (sourceDesc.getClass == targetDesc.getClass) {
        if (sourceDesc.equals(targetDesc)) {
          this.addOperation(sourceDesc, targetDesc)
          this.invokeSet(sourceDesc, targetDesc)
        } else {
          mergeSet.add(targetDesc)
        }
      }
    })

    val finalSet = new JHashSet[DatasourceDesc]()
    finalSet.addAll(mergeSet)
    finalSet
  }

  /**
   * 调用set方法设置其他字段属性
   */
  private[this] def invokeSet[T <: DatasourceDesc : ClassTag](source: T, target: T): Unit = {
    val setMethod = ReflectionUtils.getMethodByName(source.getClass, "set")
    if (setMethod != null) {
      setMethod.invoke(source, target)
    }
  }

  /**
   * 为指定的数据源添加操作集
   *
   * @param source
   * 源数据源
   * @param target
   * 目标数据源
   */
  protected[fire] def addOperation(source: DatasourceDesc, target: DatasourceDesc): Unit = {
    tryWithLog {
      LineageManager.printLog(s"1. 合并operation前：source：$source target：$target")
      // 反射获取target中的操作类型
      val targetOperationMethod = ReflectionUtils.getMethodByName(target.getClass, "operation")
      val targetOperations = new JHashSet[Operation]()
      if (targetOperationMethod != null) {
        val operation = targetOperationMethod.invoke(target)
        if (operation != null) {
          operation match {
            case operations: JHashSet[Operation] =>
              targetOperations.addAll(operations)
            case operations: mutable.Set[Operation] =>
              targetOperations.addAll(operations)
            case operations: collection.immutable.Set[Operation] =>
              targetOperations.addAll(operations)
            case _ =>
          }
        }
      }

      if (targetOperations.isEmpty) return

      // 将target中的operation几盒添加到source数据源中
      val clazz = source.getClass
      val operationMethod = ReflectionUtils.getMethodByName(clazz, "operation")
      if (operationMethod != null) {
        val operation = operationMethod.invoke(source)
        val sourceOptions = if (operation != null) operation.asInstanceOf[JHashSet[Operation]] else new JHashSet[Operation]()
        if (operation != null) {
          val methodEq = ReflectionUtils.getMethodByName(clazz, "operation_$eq")
          if (methodEq != null) {
            sourceOptions.addAll(targetOperations)
            methodEq.invoke(source, sourceOptions)
          }
        }
      }
      LineageManager.printLog(s"2. 合并operation后：source：$source")
    } (this.logger, catchLog = s"${source.getClass.getName} 血缘操作类型Set[Operation]合并失败")
  }
}
