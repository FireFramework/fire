package com.zto.fire.spark.sql

import com.zto.fire._
import com.zto.fire.common.util.{ExceptionBus, Logging}
import com.zto.fire.core.sql.SqlExtensionsParser
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}


/**
 * Spark Sql解析扩展，用于拦截执行的sql以及解析sql中的血缘
 *
 * @author ChengLong 2021-6-23 10:25:17
 * @since 2.0.0
 */
private[fire] class SparkSqlExtensionsParser(sparkSession: SparkSession, parser: ParserInterface) extends ParserInterface with Logging {

  /**
   * Parse a string to a [[LogicalPlan]].
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      this.parseExpression(sqlText)
      SparkSqlParser.sqlParse(sqlText)
      parser.parsePlan(sqlText)
    } catch {
      case e: Throwable =>
        ExceptionBus.post(e, sqlText)
        throw e
    }
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

private[fire] object SparkSqlExtensionsParser extends SqlExtensionsParser {

  /**
   * 启用自定义Sql解析器扩展
   */
  def sqlExtension(sessionBuilder: SparkSession.Builder): SparkSession.Builder = {
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    val parserBuilder: ParserBuilder = (sparkSession, parser) => new SparkSqlExtensionsParser(sparkSession, parser)
    val extBuilder: ExtensionsBuilder = { e => e.injectParser(parserBuilder) }
    sessionBuilder.withExtensions(extBuilder)
  }
}