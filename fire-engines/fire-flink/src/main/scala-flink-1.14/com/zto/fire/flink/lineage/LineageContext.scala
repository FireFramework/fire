package com.zto.fire.flink.lineage

import com.zto.fire.common.bean.lineage.LineageResult
import com.zto.fire.common.util.Constant
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelColumnOrigin, RelMetadataQueryBase}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.operations.{CatalogSinkModifyOperation, Operation}
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider
import org.apache.flink.table.planner.plan.schema.TableSourceTable

import java.util
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps


class LineageContext(tableEnv: TableEnvironmentImpl) {

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

  def buildFiledLineageResult(sinkTable: String, optRelNode: RelNode):ListBuffer[LineageResult] = {
    val targetColumnList = tableEnv.from(sinkTable)
      .getResolvedSchema
      .getColumnNames
    validateSchema(sinkTable, optRelNode, targetColumnList)
    val metadataQuery = optRelNode.getCluster.getMetadataQuery
    val resultList = ListBuffer[LineageResult]()

    for (index <- 0 until targetColumnList.size) {
      val targetColumn = targetColumnList.get(index)

      val relColumnOriginSet = metadataQuery.getColumnOrigins(optRelNode, index).asScala
      if (relColumnOriginSet.nonEmpty) {
        for (rco: RelColumnOrigin <- relColumnOriginSet) {
          // table
          val table = rco.getOriginTable
          val sourceTable = String.join(Constant.DELIMITER, table.getQualifiedName)
          // field
          val ordinal = rco.getOriginColumnOrdinal
          val fieldNames = table.asInstanceOf[TableSourceTable].catalogTable.getResolvedSchema.getColumnNames
          val sourceColumn = fieldNames.get(ordinal)
          println("----------------------------------------------------------")
          println("Source table: {}", sourceTable)
          println("Source column: {}", sourceColumn)
          if (StringUtils.isNotEmpty(rco.getTransform)) {
            println("transform: {}", rco.getTransform)
          }
          // add record
          resultList += new LineageResult(sourceTable, sourceColumn, sinkTable, targetColumn, rco.getTransform)
        }
      }
    }
    resultList


  }

  def analyzeLineage(sql: String) = {

    RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
    val parsed = parseStatement(sql)
    val sinkTable = parsed._1
    val oriRelNode = parsed._2
    buildFiledLineageResult(sinkTable, oriRelNode)
  }

  private def parseStatement(singleSql: String): Tuple2[String, RelNode] = {
    val operation = parseValidateConvert(singleSql)
    operation match {
      case sinkOperation: CatalogSinkModifyOperation =>
        val queryOperation = sinkOperation.getChild.asInstanceOf[PlannerQueryOperation]
        val relNode = queryOperation.getCalciteTree
        Tuple2(sinkOperation.getTableIdentifier.asSummaryString(), relNode)
      case _ =>
        throw new TableException("Only insert is supported now.")
    }


  }

  private def parseValidateConvert(singleSql: String) = {
    RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
    val operations: util.List[Operation] = tableEnv.getParser.parse(singleSql)
    if (operations.size() != 1) {
      throw new TableException("Unsupported SQL query! only accepts a single SQL statement.")
    }
    operations.get(0)
  }
}
