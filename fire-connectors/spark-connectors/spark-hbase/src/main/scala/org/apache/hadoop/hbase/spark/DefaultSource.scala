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

package org.apache.hadoop.hbase.spark

import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.spark.datasources.HBaseTableScanRDD
import org.apache.hadoop.hbase.spark.datasources.SerializableConfiguration
import org.apache.hadoop.hbase.types._
import org.apache.hadoop.hbase.util.{Bytes, PositionedByteRange, SimplePositionedMutableByteRange}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * DefaultSource for integration with Spark's dataframe datasources.
 * This class will produce a relationProvider based on input given to it from spark
 *
 * In all this DefaultSource support the following datasource functionality
 * - Scan range pruning through filter push down logic based on rowKeys
 * - Filter push down logic on HBase Cells
 * - Qualifier filtering based on columns used in the SparkSQL statement
 * - Type conversions of basic SQL types.  All conversions will be
 *   Through the HBase Bytes object commands.
 */
class DefaultSource extends RelationProvider with Logging {

  val TABLE_KEY:String = "hbase.table"
  val SCHEMA_COLUMNS_MAPPING_KEY:String = "hbase.columns.mapping"
  val HBASE_CONFIG_RESOURCES_LOCATIONS:String = "hbase.config.resources"
  val USE_HBASE_CONTEXT:String = "hbase.use.hbase.context"
  val PUSH_DOWN_COLUMN_FILTER:String = "hbase.push.down.column.filter"

  /**
   * Is given input from SparkSQL to construct a BaseRelation
   * @param sqlContext SparkSQL context
   * @param parameters Parameters given to us from SparkSQL
   * @return           A BaseRelation Object
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]):
  BaseRelation = {


    val tableName = parameters.get(TABLE_KEY)
    if (tableName.isEmpty)
      new IllegalArgumentException("Invalid value for " + TABLE_KEY +" '" + tableName + "'")

    val schemaMappingString = parameters.getOrElse(SCHEMA_COLUMNS_MAPPING_KEY, "")
    val hbaseConfigResources = parameters.getOrElse(HBASE_CONFIG_RESOURCES_LOCATIONS, "")
    val useHBaseReources = parameters.getOrElse(USE_HBASE_CONTEXT, "true")
    val usePushDownColumnFilter = parameters.getOrElse(PUSH_DOWN_COLUMN_FILTER, "true")

    new HBaseRelation(tableName.get,
      generateSchemaMappingMap(schemaMappingString),
      hbaseConfigResources,
      useHBaseReources.equalsIgnoreCase("true"),
      usePushDownColumnFilter.equalsIgnoreCase("true"),
      parameters)(sqlContext)
  }

  /**
   * Reads the SCHEMA_COLUMNS_MAPPING_KEY and converts it to a map of
   * SchemaQualifierDefinitions with the original sql column name as the key
   * @param schemaMappingString The schema mapping string from the SparkSQL map
   * @return                    A map of definitions keyed by the SparkSQL column name
   */
  def generateSchemaMappingMap(schemaMappingString:String):
  java.util.HashMap[String, SchemaQualifierDefinition] = {
    try {
      val columnDefinitions = schemaMappingString.split(',')
      val resultingMap = new java.util.HashMap[String, SchemaQualifierDefinition]()
      columnDefinitions.map(cd => {
        val parts = cd.trim.split(' ')

        //Make sure we get three parts
        //<ColumnName> <ColumnType> <ColumnFamily:Qualifier>
        if (parts.length == 3) {
          val hbaseDefinitionParts = if (parts(2).charAt(0) == ':') {
            Array[String]("", "key")
          } else {
            parts(2).split(':')
          }
          resultingMap.put(parts(0), new SchemaQualifierDefinition(parts(0),
            parts(1), hbaseDefinitionParts(0), hbaseDefinitionParts(1)))
        } else {
          throw new IllegalArgumentException("Invalid value for schema mapping '" + cd +
            "' should be '<columnName> <columnType> <columnFamily>:<qualifier>' " +
            "for columns and '<columnName> <columnType> :<qualifier>' for rowKeys")
        }
      })
      resultingMap
    } catch {
      case e:Exception => throw
        new IllegalArgumentException("Invalid value for " + SCHEMA_COLUMNS_MAPPING_KEY +
          " '" + schemaMappingString + "'", e )
    }
  }
}

/**
 * Implementation of Spark BaseRelation that will build up our scan logic
 * , do the scan pruning, filter push down, and value conversions
 *
 * @param tableName               HBase table that we plan to read from
 * @param schemaMappingDefinition SchemaMapping information to map HBase
 *                                Qualifiers to SparkSQL columns
 * @param configResources         Optional comma separated list of config resources
 *                                to get based on their URI
 * @param useHBaseContext         If true this will look to see if
 *                                HBaseContext.latest is populated to use that
 *                                connection information
 * @param sqlContext              SparkSQL context
 */
case class HBaseRelation (val tableName:String,
                     val schemaMappingDefinition:
                     java.util.HashMap[String, SchemaQualifierDefinition],
                     val configResources:String,
                     val useHBaseContext:Boolean,
                     val usePushDownColumnFilter:Boolean,
                     @transient parameters: Map[String, String] ) (
  @transient val sqlContext:SQLContext)
  extends BaseRelation with PrunedFilteredScan with Logging {

  // The user supplied per table parameter will overwrite global ones in SparkConf
  val blockCacheEnable = parameters.get(HBaseSparkConf.BLOCK_CACHE_ENABLE).map(_.toBoolean)
    .getOrElse(
      sqlContext.sparkContext.getConf.getBoolean(
        HBaseSparkConf.BLOCK_CACHE_ENABLE, HBaseSparkConf.defaultBlockCacheEnable))
  val cacheSize = parameters.get(HBaseSparkConf.CACHE_SIZE).map(_.toInt)
    .getOrElse(
      sqlContext.sparkContext.getConf.getInt(
      HBaseSparkConf.CACHE_SIZE, HBaseSparkConf.defaultCachingSize))
  val batchNum = parameters.get(HBaseSparkConf.BATCH_NUM).map(_.toInt)
    .getOrElse(sqlContext.sparkContext.getConf.getInt(
    HBaseSparkConf.BATCH_NUM,  HBaseSparkConf.defaultBatchNum))

  val bulkGetSize =  parameters.get(HBaseSparkConf.BULKGET_SIZE).map(_.toInt)
    .getOrElse(sqlContext.sparkContext.getConf.getInt(
    HBaseSparkConf.BULKGET_SIZE,  HBaseSparkConf.defaultBulkGetSize))

  //create or get latest HBaseContext
  val hbaseContext:HBaseContext = if (useHBaseContext) {
    LatestHBaseContextCache.latest
  } else {
    val config = HBaseConfiguration.create()
    configResources.split(",").foreach( r => config.addResource(r))
    new HBaseContext(sqlContext.sparkContext, config)
  }

  val wrappedConf = new SerializableConfiguration(hbaseContext.config)
  def hbaseConf = wrappedConf.value

  /**
   * Generates a Spark SQL schema object so Spark SQL knows what is being
   * provided by this BaseRelation
   *
   * @return schema generated from the SCHEMA_COLUMNS_MAPPING_KEY value
   */
  override def schema: StructType = {

    val metadataBuilder = new MetadataBuilder()

    val structFieldArray = new Array[StructField](schemaMappingDefinition.size())

    val schemaMappingDefinitionIt = schemaMappingDefinition.values().iterator()
    var indexCounter = 0
    while (schemaMappingDefinitionIt.hasNext) {
      val c = schemaMappingDefinitionIt.next()

      val metadata = metadataBuilder.putString("name", c.columnName).build()
      val structField =
        new StructField(c.columnName, c.columnSparkSqlType, nullable = true, metadata)

      structFieldArray(indexCounter) = structField
      indexCounter += 1
    }

    val result = new StructType(structFieldArray)
    result
  }

  /**
   * Here we are building the functionality to populate the resulting RDD[Row]
   * Here is where we will do the following:
   * - Filter push down
   * - Scan or GetList pruning
   * - Executing our scan(s) or/and GetList to generate result
   *
   * @param requiredColumns The columns that are being requested by the requesting query
   * @param filters         The filters that are being applied by the requesting query
   * @return                RDD will all the results from HBase needed for SparkSQL to
   *                        execute the query on
   */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {


    val pushDownTuple = buildPushDownPredicatesResource(filters)
    val pushDownRowKeyFilter = pushDownTuple._1
    var pushDownDynamicLogicExpression = pushDownTuple._2
    val valueArray = pushDownTuple._3

    if (!usePushDownColumnFilter) {
      pushDownDynamicLogicExpression = null
    }

    logDebug("pushDownRowKeyFilter:           " + pushDownRowKeyFilter.ranges)
    if (pushDownDynamicLogicExpression != null) {
      logDebug("pushDownDynamicLogicExpression: " +
        pushDownDynamicLogicExpression.toExpressionString)
    }
    logDebug("valueArray:                     " + valueArray.length)

    val requiredQualifierDefinitionList =
      new mutable.MutableList[SchemaQualifierDefinition]

    requiredColumns.foreach( c => {
      val definition = schemaMappingDefinition.get(c)
      requiredQualifierDefinitionList += definition
    })

    //Create a local variable so that scala doesn't have to
    // serialize the whole HBaseRelation Object
    val serializableDefinitionMap = schemaMappingDefinition

    //retain the information for unit testing checks
    DefaultSourceStaticUtils.populateLatestExecutionRules(pushDownRowKeyFilter,
      pushDownDynamicLogicExpression)

    val getList = new util.ArrayList[Get]()
    val rddList = new util.ArrayList[RDD[Row]]()

    //add points to getList
    pushDownRowKeyFilter.points.foreach(p => {
      val get = new Get(p)
      requiredQualifierDefinitionList.foreach( d => {
        if (d.columnFamilyBytes.length > 0)
          get.addColumn(d.columnFamilyBytes, d.qualifierBytes)
      })
      getList.add(get)
    })

    val pushDownFilterJava = if (usePushDownColumnFilter && pushDownDynamicLogicExpression != null) {
        Some(new SparkSQLPushDownFilter(pushDownDynamicLogicExpression,
          valueArray, requiredQualifierDefinitionList))
    } else {
      None
    }
    val hRdd = new HBaseTableScanRDD(this, hbaseContext, pushDownFilterJava, requiredQualifierDefinitionList.seq)
    pushDownRowKeyFilter.points.foreach(hRdd.addPoint(_))
    pushDownRowKeyFilter.ranges.foreach(hRdd.addRange(_))
    var resultRDD: RDD[Row] = {
      val tmp = hRdd.map{ r =>
        Row.fromSeq(requiredColumns.map(c =>
          DefaultSourceStaticUtils.getValue(c, serializableDefinitionMap, r)))
      }
      if (tmp.partitions.size > 0) {
        tmp
      } else {
        null
      }
    }

    if (resultRDD == null) {
      val scan = new Scan()
      scan.setCacheBlocks(blockCacheEnable)
      scan.setBatch(batchNum)
      scan.setCaching(cacheSize)
      requiredQualifierDefinitionList.foreach( d =>
        scan.addColumn(d.columnFamilyBytes, d.qualifierBytes))

      val rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan).map(r => {
        Row.fromSeq(requiredColumns.map(c => DefaultSourceStaticUtils.getValue(c,
          serializableDefinitionMap, r._2)))
      })
      resultRDD=rdd
    }
    resultRDD
  }

  def buildPushDownPredicatesResource(filters: Array[Filter]):
  (RowKeyFilter, DynamicLogicExpression, Array[Array[Byte]]) = {
    var superRowKeyFilter:RowKeyFilter = null
    val queryValueList = new mutable.MutableList[Array[Byte]]
    var superDynamicLogicExpression: DynamicLogicExpression = null

    filters.foreach( f => {
      val rowKeyFilter = new RowKeyFilter()
      val logicExpression = transverseFilterTree(rowKeyFilter, queryValueList, f)
      if (superDynamicLogicExpression == null) {
        superDynamicLogicExpression = logicExpression
        superRowKeyFilter = rowKeyFilter
      } else {
        superDynamicLogicExpression =
          new AndLogicExpression(superDynamicLogicExpression, logicExpression)
        superRowKeyFilter.mergeIntersect(rowKeyFilter)
      }

    })

    val queryValueArray = queryValueList.toArray

    if (superRowKeyFilter == null) {
      superRowKeyFilter = new RowKeyFilter
    }

    (superRowKeyFilter, superDynamicLogicExpression, queryValueArray)
  }

  def transverseFilterTree(parentRowKeyFilter:RowKeyFilter,
                                  valueArray:mutable.MutableList[Array[Byte]],
                                  filter:Filter): DynamicLogicExpression = {
    filter match {

      case EqualTo(attr, value) =>
        val columnDefinition = schemaMappingDefinition.get(attr)
        if (columnDefinition != null) {
          if (columnDefinition.columnFamily.isEmpty) {
            parentRowKeyFilter.mergeIntersect(new RowKeyFilter(
              DefaultSourceStaticUtils.getByteValue(attr,
                schemaMappingDefinition, value.toString), null))
          }
          val byteValue =
            DefaultSourceStaticUtils.getByteValue(attr,
              schemaMappingDefinition, value.toString)
          valueArray += byteValue
        }
        new EqualLogicExpression(attr, valueArray.length - 1, false)
      case LessThan(attr, value) =>
        val columnDefinition = schemaMappingDefinition.get(attr)
        if (columnDefinition != null) {
          if (columnDefinition.columnFamily.isEmpty) {
            parentRowKeyFilter.mergeIntersect(new RowKeyFilter(null,
              new ScanRange(DefaultSourceStaticUtils.getByteValue(attr,
                schemaMappingDefinition, value.toString), false,
                new Array[Byte](0), true)))
          }
          val byteValue =
            DefaultSourceStaticUtils.getByteValue(attr,
              schemaMappingDefinition, value.toString)
          valueArray += byteValue
        }
        new LessThanLogicExpression(attr, valueArray.length - 1)
      case GreaterThan(attr, value) =>
        val columnDefinition = schemaMappingDefinition.get(attr)
        if (columnDefinition != null) {
          if (columnDefinition.columnFamily.isEmpty) {
            parentRowKeyFilter.mergeIntersect(new RowKeyFilter(null,
              new ScanRange(null, true, DefaultSourceStaticUtils.getByteValue(attr,
                schemaMappingDefinition, value.toString), false)))
          }
          val byteValue =
            DefaultSourceStaticUtils.getByteValue(attr,
              schemaMappingDefinition, value.toString)
          valueArray += byteValue
        }
        new GreaterThanLogicExpression(attr, valueArray.length - 1)
      case LessThanOrEqual(attr, value) =>
        val columnDefinition = schemaMappingDefinition.get(attr)
        if (columnDefinition != null) {
          if (columnDefinition.columnFamily.isEmpty) {
            parentRowKeyFilter.mergeIntersect(new RowKeyFilter(null,
              new ScanRange(DefaultSourceStaticUtils.getByteValue(attr,
                schemaMappingDefinition, value.toString), true,
                new Array[Byte](0), true)))
          }
          val byteValue =
            DefaultSourceStaticUtils.getByteValue(attr,
              schemaMappingDefinition, value.toString)
          valueArray += byteValue
        }
        new LessThanOrEqualLogicExpression(attr, valueArray.length - 1)
      case GreaterThanOrEqual(attr, value) =>
        val columnDefinition = schemaMappingDefinition.get(attr)
        if (columnDefinition != null) {
          if (columnDefinition.columnFamily.isEmpty) {
            parentRowKeyFilter.mergeIntersect(new RowKeyFilter(null,
              new ScanRange(null, true, DefaultSourceStaticUtils.getByteValue(attr,
                schemaMappingDefinition, value.toString), true)))
          }
          val byteValue =
            DefaultSourceStaticUtils.getByteValue(attr,
              schemaMappingDefinition, value.toString)
          valueArray += byteValue

        }
        new GreaterThanOrEqualLogicExpression(attr, valueArray.length - 1)
      case Or(left, right) =>
        val leftExpression = transverseFilterTree(parentRowKeyFilter, valueArray, left)
        val rightSideRowKeyFilter = new RowKeyFilter
        val rightExpression = transverseFilterTree(rightSideRowKeyFilter, valueArray, right)

        parentRowKeyFilter.mergeUnion(rightSideRowKeyFilter)

        new OrLogicExpression(leftExpression, rightExpression)
      case And(left, right) =>

        val leftExpression = transverseFilterTree(parentRowKeyFilter, valueArray, left)
        val rightSideRowKeyFilter = new RowKeyFilter
        val rightExpression = transverseFilterTree(rightSideRowKeyFilter, valueArray, right)
        parentRowKeyFilter.mergeIntersect(rightSideRowKeyFilter)

        new AndLogicExpression(leftExpression, rightExpression)
      case IsNull(attr) =>
        new IsNullLogicExpression(attr, false)
      case IsNotNull(attr) =>
        new IsNullLogicExpression(attr, true)
      case _ =>
        new PassThroughLogicExpression
    }
  }
}

/**
 * Construct to contains column data that spend SparkSQL and HBase
 *
 * @param columnName   SparkSQL column name
 * @param colType      SparkSQL column type
 * @param columnFamily HBase column family
 * @param qualifier    HBase qualifier name
 */
case class SchemaQualifierDefinition(columnName:String,
                          colType:String,
                          columnFamily:String,
                          qualifier:String) extends Serializable {
  val columnFamilyBytes = Bytes.toBytes(columnFamily)
  val qualifierBytes = Bytes.toBytes(qualifier)
  val columnSparkSqlType:DataType = if (colType.equals("BOOLEAN")) BooleanType
    else if (colType.equals("TINYINT")) IntegerType
    else if (colType.equals("INT")) IntegerType
    else if (colType.equals("BIGINT")) LongType
    else if (colType.equals("FLOAT")) FloatType
    else if (colType.equals("DOUBLE")) DoubleType
    else if (colType.equals("STRING")) StringType
    else if (colType.equals("TIMESTAMP")) TimestampType
    else if (colType.equals("DECIMAL")) StringType
    else throw new IllegalArgumentException("Unsupported column type :" + colType)
}

/**
 * Construct to contain a single scan ranges information.  Also
 * provide functions to merge with other scan ranges through AND
 * or OR operators
 *
 * @param upperBound          Upper bound of scan
 * @param isUpperBoundEqualTo Include upper bound value in the results
 * @param lowerBound          Lower bound of scan
 * @param isLowerBoundEqualTo Include lower bound value in the results
 */
class ScanRange(var upperBound:Array[Byte], var isUpperBoundEqualTo:Boolean,
                var lowerBound:Array[Byte], var isLowerBoundEqualTo:Boolean)
  extends Serializable {

  /**
   * Function to merge another scan object through a AND operation
   * @param other Other scan object
   */
  def mergeIntersect(other:ScanRange): Unit = {
    val upperBoundCompare = compareRange(upperBound, other.upperBound)
    val lowerBoundCompare = compareRange(lowerBound, other.lowerBound)

    upperBound = if (upperBoundCompare <0) upperBound else other.upperBound
    lowerBound = if (lowerBoundCompare >0) lowerBound else other.lowerBound

    isLowerBoundEqualTo = if (lowerBoundCompare == 0)
      isLowerBoundEqualTo && other.isLowerBoundEqualTo
    else isLowerBoundEqualTo

    isUpperBoundEqualTo = if (upperBoundCompare == 0)
      isUpperBoundEqualTo && other.isUpperBoundEqualTo
    else isUpperBoundEqualTo
  }

  /**
   * Function to merge another scan object through a OR operation
   * @param other Other scan object
   */
  def mergeUnion(other:ScanRange): Unit = {

    val upperBoundCompare = compareRange(upperBound, other.upperBound)
    val lowerBoundCompare = compareRange(lowerBound, other.lowerBound)

    upperBound = if (upperBoundCompare >0) upperBound else other.upperBound
    lowerBound = if (lowerBoundCompare <0) lowerBound else other.lowerBound

    isLowerBoundEqualTo = if (lowerBoundCompare == 0)
      isLowerBoundEqualTo || other.isLowerBoundEqualTo
    else if (lowerBoundCompare < 0) isLowerBoundEqualTo else other.isLowerBoundEqualTo

    isUpperBoundEqualTo = if (upperBoundCompare == 0)
      isUpperBoundEqualTo || other.isUpperBoundEqualTo
    else if (upperBoundCompare < 0) other.isUpperBoundEqualTo else isUpperBoundEqualTo
  }

  /**
   * Common function to see if this scan over laps with another
   *
   * Reference Visual
   *
   * A                           B
   * |---------------------------|
   *   LL--------------LU
   *        RL--------------RU
   *
   * A = lowest value is byte[0]
   * B = highest value is null
   * LL = Left Lower Bound
   * LU = Left Upper Bound
   * RL = Right Lower Bound
   * RU = Right Upper Bound
   *
   * @param other Other scan object
   * @return      True is overlap false is not overlap
   */
  def getOverLapScanRange(other:ScanRange): ScanRange = {

    var leftRange:ScanRange = null
    var rightRange:ScanRange = null

    //First identify the Left range
    // Also lower bound can't be null
    if (compareRange(lowerBound, other.lowerBound) < 0 ||
      compareRange(upperBound, other.upperBound) < 0) {
      leftRange = this
      rightRange = other
    } else {
      leftRange = other
      rightRange = this
    }

    //Then see if leftRange goes to null or if leftRange.upperBound
    // upper is greater or equals to rightRange.lowerBound
    if (leftRange.upperBound == null ||
      Bytes.compareTo(leftRange.upperBound, rightRange.lowerBound) >= 0) {
      new ScanRange(leftRange.upperBound, leftRange.isUpperBoundEqualTo, rightRange.lowerBound, rightRange.isLowerBoundEqualTo)
    } else {
      null
    }
  }

  /**
   * Special compare logic because we can have null values
   * for left or right bound
   *
   * @param left  Left byte array
   * @param right Right byte array
   * @return      0 for equals 1 is left is greater and -1 is right is greater
   */
  def compareRange(left:Array[Byte], right:Array[Byte]): Int = {
    if (left == null && right == null) 0
    else if (left == null && right != null) 1
    else if (left != null && right == null) -1
    else Bytes.compareTo(left, right)
  }

  /**
   *
   * @return
   */
  def containsPoint(point:Array[Byte]): Boolean = {
    val lowerCompare = compareRange(point, lowerBound)
    val upperCompare = compareRange(point, upperBound)

    ((isLowerBoundEqualTo && lowerCompare >= 0) ||
      (!isLowerBoundEqualTo && lowerCompare > 0)) &&
      ((isUpperBoundEqualTo && upperCompare <= 0) ||
        (!isUpperBoundEqualTo && upperCompare < 0))

  }
  override def toString:String = {
    "ScanRange:(upperBound:" + Bytes.toString(upperBound) +
      ",isUpperBoundEqualTo:" + isUpperBoundEqualTo + ",lowerBound:" +
      Bytes.toString(lowerBound) + ",isLowerBoundEqualTo:" + isLowerBoundEqualTo + ")"
  }
}

/**
 * Contains information related to a filters for a given column.
 * This can contain many ranges or points.
 *
 * @param currentPoint the initial point when the filter is created
 * @param currentRange the initial scanRange when the filter is created
 */
class ColumnFilter (currentPoint:Array[Byte] = null,
                     currentRange:ScanRange = null,
                     var points:mutable.MutableList[Array[Byte]] =
                     new mutable.MutableList[Array[Byte]](),
                     var ranges:mutable.MutableList[ScanRange] =
                     new mutable.MutableList[ScanRange]() ) extends Serializable {
  //Collection of ranges
  if (currentRange != null ) ranges.+=(currentRange)

  //Collection of points
  if (currentPoint != null) points.+=(currentPoint)

  /**
   * This will validate a give value through the filter's points and/or ranges
   * the result will be if the value passed the filter
   *
   * @param value       Value to be validated
   * @param valueOffSet The offset of the value
   * @param valueLength The length of the value
   * @return            True is the value passes the filter false if not
   */
  def validate(value:Array[Byte], valueOffSet:Int, valueLength:Int):Boolean = {
    var result = false

    points.foreach( p => {
      if (Bytes.equals(p, 0, p.length, value, valueOffSet, valueLength)) {
        result = true
      }
    })

    ranges.foreach( r => {
      val upperBoundPass = r.upperBound == null ||
        (r.isUpperBoundEqualTo &&
          Bytes.compareTo(r.upperBound, 0, r.upperBound.length,
            value, valueOffSet, valueLength) >= 0) ||
        (!r.isUpperBoundEqualTo &&
          Bytes.compareTo(r.upperBound, 0, r.upperBound.length,
            value, valueOffSet, valueLength) > 0)

      val lowerBoundPass = r.lowerBound == null || r.lowerBound.length == 0
        (r.isLowerBoundEqualTo &&
          Bytes.compareTo(r.lowerBound, 0, r.lowerBound.length,
            value, valueOffSet, valueLength) <= 0) ||
        (!r.isLowerBoundEqualTo &&
          Bytes.compareTo(r.lowerBound, 0, r.lowerBound.length,
            value, valueOffSet, valueLength) < 0)

      result = result || (upperBoundPass && lowerBoundPass)
    })
    result
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a OR operator
   *
   * @param other Filter to merge
   */
  def mergeUnion(other:ColumnFilter): Unit = {
    other.points.foreach( p => points += p)

    other.ranges.foreach( otherR => {
      var doesOverLap = false
      ranges.foreach{ r =>
        if (r.getOverLapScanRange(otherR) != null) {
          r.mergeUnion(otherR)
          doesOverLap = true
        }}
      if (!doesOverLap) ranges.+=(otherR)
    })
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a AND operator
   *
   * @param other Filter to merge
   */
  def mergeIntersect(other:ColumnFilter): Unit = {
    val survivingPoints = new mutable.MutableList[Array[Byte]]()
    points.foreach( p => {
      other.points.foreach( otherP => {
        if (Bytes.equals(p, otherP)) {
          survivingPoints.+=(p)
        }
      })
    })
    points = survivingPoints

    val survivingRanges = new mutable.MutableList[ScanRange]()

    other.ranges.foreach( otherR => {
      ranges.foreach( r => {
        if (r.getOverLapScanRange(otherR) != null) {
          r.mergeIntersect(otherR)
          survivingRanges += r
        }
      })
    })
    ranges = survivingRanges
  }

  override def toString:String = {
    val strBuilder = new StringBuilder
    strBuilder.append("(points:(")
    var isFirst = true
    points.foreach( p => {
      if (isFirst) isFirst = false
      else strBuilder.append(",")
      strBuilder.append(Bytes.toString(p))
    })
    strBuilder.append("),ranges:")
    isFirst = true
    ranges.foreach( r => {
      if (isFirst) isFirst = false
      else strBuilder.append(",")
      strBuilder.append(r)
    })
    strBuilder.append("))")
    strBuilder.toString()
  }
}

/**
 * A collection of ColumnFilters indexed by column names.
 *
 * Also contains merge commends that will consolidate the filters
 * per column name
 */
class ColumnFilterCollection {
  val columnFilterMap = new mutable.HashMap[String, ColumnFilter]

  def clear(): Unit = {
    columnFilterMap.clear()
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a OR operator.  This will merge a single columns filter
   *
   * @param column The column to be merged
   * @param other  The other ColumnFilter object to merge
   */
  def mergeUnion(column:String, other:ColumnFilter): Unit = {
    val existingFilter = columnFilterMap.get(column)
    if (existingFilter.isEmpty) {
      columnFilterMap.+=((column, other))
    } else {
      existingFilter.get.mergeUnion(other)
    }
  }

  /**
   * This will allow us to merge all filters in the existing collection
   * to the filters in the other collection.  All merges are done as a result
   * of a OR operator
   *
   * @param other The other Column Filter Collection to be merged
   */
  def mergeUnion(other:ColumnFilterCollection): Unit = {
    other.columnFilterMap.foreach( e => {
      mergeUnion(e._1, e._2)
    })
  }

  /**
   * This will allow us to merge all filters in the existing collection
   * to the filters in the other collection.  All merges are done as a result
   * of a AND operator
   *
   * @param other The column filter from the other collection
   */
  def mergeIntersect(other:ColumnFilterCollection): Unit = {
    other.columnFilterMap.foreach( e => {
      val existingColumnFilter = columnFilterMap.get(e._1)
      if (existingColumnFilter.isEmpty) {
        columnFilterMap += e
      } else {
        existingColumnFilter.get.mergeIntersect(e._2)
      }
    })
  }

  /**
   * This will collect all the filter information in a way that is optimized
   * for the HBase filter commend.  Allowing the filter to be accessed
   * with columnFamily and qualifier information
   *
   * @param schemaDefinitionMap Schema Map that will help us map the right filters
   *                            to the correct columns
   * @return                    HashMap oc column filters
   */
  def generateFamilyQualifiterFilterMap(schemaDefinitionMap:
                                        java.util.HashMap[String,
                                          SchemaQualifierDefinition]):
  util.HashMap[ColumnFamilyQualifierMapKeyWrapper, ColumnFilter] = {
    val familyQualifierFilterMap =
      new util.HashMap[ColumnFamilyQualifierMapKeyWrapper, ColumnFilter]()

    columnFilterMap.foreach( e => {
      val definition = schemaDefinitionMap.get(e._1)
      //Don't add rowKeyFilter
      if (definition.columnFamilyBytes.size > 0) {
        familyQualifierFilterMap.put(
          new ColumnFamilyQualifierMapKeyWrapper(
            definition.columnFamilyBytes, 0, definition.columnFamilyBytes.length,
            definition.qualifierBytes, 0, definition.qualifierBytes.length), e._2)
      }
    })
    familyQualifierFilterMap
  }

  override def toString:String = {
    val strBuilder = new StringBuilder
    columnFilterMap.foreach( e => strBuilder.append(e))
    strBuilder.toString()
  }
}

/**
 * Status object to store static functions but also to hold last executed
 * information that can be used for unit testing.
 */
object DefaultSourceStaticUtils {

  val rawInteger = new RawInteger
  val rawLong = new RawLong
  val rawFloat = new RawFloat
  val rawDouble = new RawDouble
  val rawString = RawString.ASCENDING

  val byteRange = new ThreadLocal[PositionedByteRange]{
    override def initialValue(): PositionedByteRange = {
      val range = new SimplePositionedMutableByteRange()
      range.setOffset(0)
      range.setPosition(0)
    }
  }

  def getFreshByteRange(bytes:Array[Byte]): PositionedByteRange = {
    getFreshByteRange(bytes, 0, bytes.length)
  }

  def getFreshByteRange(bytes:Array[Byte],  offset:Int = 0, length:Int):
  PositionedByteRange = {
    byteRange.get().set(bytes).setLength(length).setOffset(offset)
  }

  //This will contain the last 5 filters and required fields used in buildScan
  // These values can be used in unit testing to make sure we are converting
  // The Spark SQL input correctly
  val lastFiveExecutionRules =
    new ConcurrentLinkedQueue[ExecutionRuleForUnitTesting]()

  /**
   * This method is to populate the lastFiveExecutionRules for unit test perposes
   * This method is not thread safe.
   *
   * @param rowKeyFilter           The rowKey Filter logic used in the last query
   * @param dynamicLogicExpression The dynamicLogicExpression used in the last query
   */
  def populateLatestExecutionRules(rowKeyFilter: RowKeyFilter,
                                   dynamicLogicExpression: DynamicLogicExpression):Unit = {
    lastFiveExecutionRules.add(new ExecutionRuleForUnitTesting(
      rowKeyFilter, dynamicLogicExpression))
    while (lastFiveExecutionRules.size() > 5) {
      lastFiveExecutionRules.poll()
    }
  }

  /**
   * This method will convert the result content from HBase into the
   * SQL value type that is requested by the Spark SQL schema definition
   *
   * @param columnName              The name of the SparkSQL Column
   * @param schemaMappingDefinition The schema definition map
   * @param r                       The result object from HBase
   * @return                        The converted object type
   */
  def getValue(columnName: String,
               schemaMappingDefinition:
               java.util.HashMap[String, SchemaQualifierDefinition],
               r: Result): Any = {

    val columnDef = schemaMappingDefinition.get(columnName)

    if (columnDef == null) throw new IllegalArgumentException("Unknown column:" + columnName)


    if (columnDef.columnFamilyBytes.isEmpty) {
      val row = r.getRow

      columnDef.columnSparkSqlType match {
        case IntegerType => rawInteger.decode(getFreshByteRange(row))
        case LongType => rawLong.decode(getFreshByteRange(row))
        case FloatType => rawFloat.decode(getFreshByteRange(row))
        case DoubleType => rawDouble.decode(getFreshByteRange(row))
        case StringType => rawString.decode(getFreshByteRange(row))
        case TimestampType => rawLong.decode(getFreshByteRange(row))
        case _ => Bytes.toString(row)
      }
    } else {
      val cellByteValue =
        r.getColumnLatestCell(columnDef.columnFamilyBytes, columnDef.qualifierBytes)
      if (cellByteValue == null) null
      else columnDef.columnSparkSqlType match {
        case IntegerType => rawInteger.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case LongType => rawLong.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case FloatType => rawFloat.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case DoubleType => rawDouble.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case StringType => Bytes.toString(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength)
        case TimestampType => rawLong.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case _ => Bytes.toString(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength)
      }
    }
  }

  /**
   * This will convert the value from SparkSQL to be stored into HBase using the
   * right byte Type
   *
   * @param columnName              SparkSQL column name
   * @param schemaMappingDefinition Schema definition map
   * @param value                   String value from SparkSQL
   * @return                        Returns the byte array to go into HBase
   */
  def getByteValue(columnName: String,
                   schemaMappingDefinition:
                   java.util.HashMap[String, SchemaQualifierDefinition],
                   value: String): Array[Byte] = {

    val columnDef = schemaMappingDefinition.get(columnName)

    if (columnDef == null) {
      throw new IllegalArgumentException("Unknown column:" + columnName)
    } else {
      columnDef.columnSparkSqlType match {
        case IntegerType =>
          val result = new Array[Byte](Bytes.SIZEOF_INT)
          val localDataRange = getFreshByteRange(result)
          rawInteger.encode(localDataRange, value.toInt)
          localDataRange.getBytes
        case LongType =>
          val result = new Array[Byte](Bytes.SIZEOF_LONG)
          val localDataRange = getFreshByteRange(result)
          rawLong.encode(localDataRange, value.toLong)
          localDataRange.getBytes
        case FloatType =>
          val result = new Array[Byte](Bytes.SIZEOF_FLOAT)
          val localDataRange = getFreshByteRange(result)
          rawFloat.encode(localDataRange, value.toFloat)
          localDataRange.getBytes
        case DoubleType =>
          val result = new Array[Byte](Bytes.SIZEOF_DOUBLE)
          val localDataRange = getFreshByteRange(result)
          rawDouble.encode(localDataRange, value.toDouble)
          localDataRange.getBytes
        case StringType =>
          Bytes.toBytes(value)
        case TimestampType =>
          val result = new Array[Byte](Bytes.SIZEOF_LONG)
          val localDataRange = getFreshByteRange(result)
          rawLong.encode(localDataRange, value.toLong)
          localDataRange.getBytes

        case _ => Bytes.toBytes(value)
      }
    }
  }
}

/**
 * Contains information related to a filters for a given column.
 * This can contain many ranges or points.
 *
 * @param currentPoint the initial point when the filter is created
 * @param currentRange the initial scanRange when the filter is created
 */
class RowKeyFilter (currentPoint:Array[Byte] = null,
                    currentRange:ScanRange =
                    new ScanRange(null, true, new Array[Byte](0), true),
                    var points:mutable.MutableList[Array[Byte]] =
                    new mutable.MutableList[Array[Byte]](),
                    var ranges:mutable.MutableList[ScanRange] =
                    new mutable.MutableList[ScanRange]() ) extends Serializable {
  //Collection of ranges
  if (currentRange != null ) ranges.+=(currentRange)

  //Collection of points
  if (currentPoint != null) points.+=(currentPoint)

  /**
   * This will validate a give value through the filter's points and/or ranges
   * the result will be if the value passed the filter
   *
   * @param value       Value to be validated
   * @param valueOffSet The offset of the value
   * @param valueLength The length of the value
   * @return            True is the value passes the filter false if not
   */
  def validate(value:Array[Byte], valueOffSet:Int, valueLength:Int):Boolean = {
    var result = false

    points.foreach( p => {
      if (Bytes.equals(p, 0, p.length, value, valueOffSet, valueLength)) {
        result = true
      }
    })

    ranges.foreach( r => {
      val upperBoundPass = r.upperBound == null ||
        (r.isUpperBoundEqualTo &&
          Bytes.compareTo(r.upperBound, 0, r.upperBound.length,
            value, valueOffSet, valueLength) >= 0) ||
        (!r.isUpperBoundEqualTo &&
          Bytes.compareTo(r.upperBound, 0, r.upperBound.length,
            value, valueOffSet, valueLength) > 0)

      val lowerBoundPass = r.lowerBound == null || r.lowerBound.length == 0
      (r.isLowerBoundEqualTo &&
        Bytes.compareTo(r.lowerBound, 0, r.lowerBound.length,
          value, valueOffSet, valueLength) <= 0) ||
        (!r.isLowerBoundEqualTo &&
          Bytes.compareTo(r.lowerBound, 0, r.lowerBound.length,
            value, valueOffSet, valueLength) < 0)

      result = result || (upperBoundPass && lowerBoundPass)
    })
    result
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a OR operator
   *
   * @param other Filter to merge
   */
  def mergeUnion(other:RowKeyFilter): Unit = {
    other.points.foreach( p => points += p)

    other.ranges.foreach( otherR => {
      var doesOverLap = false
      ranges.foreach{ r =>
        if (r.getOverLapScanRange(otherR) != null) {
          r.mergeUnion(otherR)
          doesOverLap = true
        }}
      if (!doesOverLap) ranges.+=(otherR)
    })
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a AND operator
   *
   * @param other Filter to merge
   */
  def mergeIntersect(other:RowKeyFilter): Unit = {
    val survivingPoints = new mutable.MutableList[Array[Byte]]()
    val didntSurviveFirstPassPoints = new mutable.MutableList[Array[Byte]]()
    if (points == null || points.length == 0) {
      other.points.foreach( otherP => {
        didntSurviveFirstPassPoints += otherP
      })
    } else {
      points.foreach(p => {
        if (other.points.length == 0) {
          didntSurviveFirstPassPoints += p
        } else {
          other.points.foreach(otherP => {
            if (Bytes.equals(p, otherP)) {
              survivingPoints += p
            } else {
              didntSurviveFirstPassPoints += p
            }
          })
        }
      })
    }

    val survivingRanges = new mutable.MutableList[ScanRange]()

    if (ranges.length == 0) {
      didntSurviveFirstPassPoints.foreach(p => {
          survivingPoints += p
      })
    } else {
      ranges.foreach(r => {
        other.ranges.foreach(otherR => {
          val overLapScanRange = r.getOverLapScanRange(otherR)
          if (overLapScanRange != null) {
            survivingRanges += overLapScanRange
          }
        })
        didntSurviveFirstPassPoints.foreach(p => {
          if (r.containsPoint(p)) {
            survivingPoints += p
          }
        })
      })
    }
    points = survivingPoints
    ranges = survivingRanges
  }

  override def toString:String = {
    val strBuilder = new StringBuilder
    strBuilder.append("(points:(")
    var isFirst = true
    points.foreach( p => {
      if (isFirst) isFirst = false
      else strBuilder.append(",")
      strBuilder.append(Bytes.toString(p))
    })
    strBuilder.append("),ranges:")
    isFirst = true
    ranges.foreach( r => {
      if (isFirst) isFirst = false
      else strBuilder.append(",")
      strBuilder.append(r)
    })
    strBuilder.append("))")
    strBuilder.toString()
  }
}



class ExecutionRuleForUnitTesting(val rowKeyFilter: RowKeyFilter,
                                  val dynamicLogicExpression: DynamicLogicExpression)
