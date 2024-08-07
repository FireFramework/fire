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
package com.zto.fire.flink.lineage

import com.zto.fire.common.bean.lineage.LineageResult
import com.zto.fire.common.util.Constant
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelColumnOrigin, RelMetadataQueryBase}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.operations.{Operation, SinkModifyOperation}
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider
import org.apache.flink.table.planner.plan.schema.TableSourceTable

import java.util
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps


class LineageContext(tableEnv: TableEnvironmentImpl) {

  /**
   * Sql 字段检验
   */
  private def validateSchema(sinkTable: String, relNode: RelNode, sinkFieldList: util.List[String]): Unit = {
    val queryFieldList = relNode.getRowType.getFieldNames
    if (queryFieldList.size() != sinkFieldList.size()) {
      throw new ValidationException(
        String.format(
          "Column types of query result and sink for %s do not match.\n"
            + "Query schema: %s\n"
            + "Sink schema:  %s",
          sinkTable, queryFieldList, sinkFieldList))
    }
  }

  /**
   * 获取血缘关系
   * @param sinkTable
   * @param optRelNode
   */
  def buildFiledLineageResult(sinkTable: String, optRelNode: RelNode): ListBuffer[LineageResult] = {
    val targetColumnList = tableEnv.from(sinkTable)
      .getResolvedSchema
      .getColumnNames
    validateSchema(sinkTable, optRelNode, targetColumnList)
    val metadataQuery = optRelNode.getCluster.getMetadataQuery
    val resultList = ListBuffer[LineageResult]()

    for (index <- 0 until targetColumnList.size) {
      val targetColumn = targetColumnList.get(index)

      val relColumnOriginSet = metadataQuery.getColumnOrigins(optRelNode, index)
      import scala.collection.JavaConversions._

      if (!relColumnOriginSet.nonEmpty) {
        for (rco: RelColumnOrigin <- relColumnOriginSet) {
          // table
          val table = rco.getOriginTable
          val sourceTable = String.join(Constant.DELIMITER, table.getQualifiedName)
          // field
          val ordinal = rco.getOriginColumnOrdinal
          val fieldNames = table.asInstanceOf[TableSourceTable].contextResolvedTable.getResolvedSchema.getColumnNames
          val sourceColumn = fieldNames.get(ordinal)
          // add record
          resultList += new LineageResult(sourceTable, sourceColumn, sinkTable, targetColumn, rco.getTransform)
        }
      }
    }
    resultList
  }

  /**
   * 获取血缘关系
   * @param sql INSERT INTO
   * 1、获取 RelNode
   * 2、根据RelNode 构造血缘
   */
  def analyzeLineage(sql: String) = {

    RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
    val parsed = parseStatement(sql)
    val sinkTable = parsed._1
    val oriRelNode = parsed._2
    buildFiledLineageResult(sinkTable, oriRelNode)
  }

  /**
   * 根据SqlNode和元数据信息构建关系表达式RelNode树
   * @param singleSql INSERT INTO
   */
  private def parseStatement(singleSql: String): Tuple2[String, RelNode] = {
    val operation = parseValidateConvert(singleSql)
    operation match {
      case sinkOperation: SinkModifyOperation =>
        val queryOperation = sinkOperation.getChild.asInstanceOf[PlannerQueryOperation]
        val relNode = queryOperation.getCalciteTree
        Tuple2(sinkOperation.getContextResolvedTable.getIdentifier.asSummaryString(), relNode)
      case _ =>
        throw new TableException("Only insert is supported now.")
    }
  }

  /**
   * 获取Sql对应的 Operation 类型
   * @param singleSql INSERT INTO
   */
  private def parseValidateConvert(singleSql: String) = {
    RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
    val operations: util.List[Operation] = tableEnv.getParser.parse(singleSql)
    if (operations.size() != 1) {
      throw new TableException("Unsupported SQL query! only accepts a single SQL statement.")
    }
    operations.get(0)
  }
}
