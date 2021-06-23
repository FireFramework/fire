package com.zto.fire.spark.sql

import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.common.util.ThreadUtils
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Spark Sql解析扩展，用于拦截执行的sql以及解析sql中的血缘
 *
 * @author ChengLong 2021-6-23 10:25:17
 * @since 2.0.0
 */
class SparkSqlExtensionsParser(parser: ParserInterface) extends ParserInterface {
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)
  private[fire] lazy val sql = mutable.HashSet[String]()
  private[this] lazy val threadPool = ThreadUtils.createThreadPool("Spark Sql Parser", ThreadPoolType.SCHEDULED)

  /**
   * Parse a string to a [[LogicalPlan]].
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    sql += sqlText
    parser.parsePlan(sqlText)
  }

  ThreadUtils.runAsThreadLoop(threadPool, this.process, 1)

  def process: Unit = {
    sql.foreach(SparkSqlParser.sqlParser)
  }

  /**
   * Parse a string to an [[Expression]].
   */
  override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)

  /**
   * Parse a string to a [[TableIdentifier]].
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parser.parseTableIdentifier(sqlText)

  /**
   * Parse a string to a [[FunctionIdentifier]].
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = parser.parseFunctionIdentifier(sqlText)

  /**
   * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated
   * list of field definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = parser.parseTableSchema(sqlText)

  /**
   * Parse a string to a [[DataType]].
   */
  override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)

  /**
   * Parse a string to a multi-part identifier.
   */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = parser.parseMultipartIdentifier(sqlText)

  /**
   * Parse a string to a raw [[DataType]] without CHAR/VARCHAR replacement.
   */
  override def parseRawDataType(sqlText: String): DataType = parser.parseRawDataType(sqlText)
}