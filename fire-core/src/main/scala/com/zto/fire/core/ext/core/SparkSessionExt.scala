package com.zto.fire.core.ext.core

import java.sql.Connection
import java.util.Properties

import com.zto.fire.common.bean.{BaseLogging, HBaseBaseBean}
import com.zto.fire.common.db.{HBaseOper, JdbcOper, QueryCallback}
import com.zto.fire.common.util.GlobalConstants.FireConf
import com.zto.fire.common.util.{GlobalConstants, KafkaUtils, ValueUtils}
import com.zto.fire.core.bridge.HBaseSparkBridge
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.ext.module.HBaseContextExt
import com.zto.fire.core.udf.UDFs
import com.zto.fire.core.util.{SingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{Get, Result, Scan}
import org.apache.hadoop.hbase.filter.{Filter, FilterList}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * SparkContext扩展
 *
 * @param spark
 * sparkSession对象
 * @author ChengLong 2019-5-18 10:51:19
 */
class SparkSessionExt(spark: SparkSession) extends BaseLogging {

  import spark.implicits._

  // 获取单例的HBaseContext对象
  lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(spark.sparkContext)
  val sc: SparkContext = spark.sparkContext

  /**
   * 根据给定的集合，创建rdd
   *
   * @param seq
   * seq
   * @param numSlices
   * 分区数
   * @return
   * RDD
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
    this.sc.parallelize(seq, numSlices)
  }

  /**
   * 批量注册udf函数，包含系统内置的与用户自定义的
   */
  def registerUDF(): SparkSession = {
    UDFs.registerSysUDF(spark)
    spark
  }

  // ----------------------------------- Spark SQL 相关API ----------------------------------- //

  /**
   * 用于判断当前SparkSession下临时表或Hive表是否存在
   *
   * @param tableName
   * 表名
   * @return
   * true：存在 false：不存在
   */
  def tableExists(tableName: String): Boolean = {
    this.spark.catalog.tableExists(tableName)
  }

  /**
   * 用于判断当前SparkSession下临时表或Hive表是否存在
   *
   * @param tableName
   * 表名
   * @return
   * true：存在 false：不存在
   */
  def tableExists(dbName: String, tableName: String): Boolean = {
    this.spark.catalog.tableExists(dbName, tableName)
  }

  /**
   * 执行一段Hive QL语句，注册为临时表，持久化到hive中
   *
   * @param sqlStr
   * sql语句
   * @param tmpTableName
   * 临时表名
   * @param saveMode
   * 持久化的模式，默认为Overwrite
   * @param cache
   * 默认缓存表
   * @return
   * 生成的DataFrame
   */
  def sqlForPersistent(sqlStr: String, tmpTableName: String, partitionName: String, saveMode: SaveMode = GlobalConstants.SparkConf.saveMode, cache: Boolean = true): DataFrame = {
    spark.sqlContext.sqlForPersistent(sqlStr, tmpTableName, partitionName, saveMode, cache)
  }

  /**
   * 执行一段Hive QL语句，注册为临时表，并cache
   *
   * @param sqlStr
   * SQL语句
   * @param tmpTableName
   * 临时表名
   * @return
   * 生成的DataFrame
   */
  def sqlForCache(sqlStr: String, tmpTableName: String): DataFrame = {
    spark.sqlContext.sqlForCache(sqlStr, tmpTableName)
  }

  /**
   * 执行一段Hive QL语句，注册为临时表
   *
   * @param sqlStr
   * SQL语句
   * @param tmpTableName
   * 临时表名
   * @return
   * 生成的DataFrame
   */
  def sqlNoCache(sqlStr: String, tmpTableName: String): DataFrame = {
    spark.sqlContext.sqlNoCache(sqlStr, tmpTableName)
  }

  /**
   * 批量缓存多张表
   *
   * @param tables
   * 多个表名
   */
  def cacheTables(tables: String*): Unit = {
    spark.sqlContext.cacheTables(tables: _*)
  }

  /**
   * 判断表是否被缓存
   *
   * @param tableName
   * 表名
   * @return
   */
  def isCached(tableName: String): Boolean = {
    this.spark.sqlContext.isCached(tableName)
  }

  /**
   * 删除指定的hive表
   *
   * @param tableNames
   * 多个表名
   */
  def dropHiveTable(tableNames: String*): Unit = {
    spark.sqlContext.dropHiveTable(tableNames: _*)
  }

  /**
   * 为指定表添加分区
   *
   * @param tableName
   * 表名
   * @param partitions
   * 分区
   */
  def addPartitions(tableName: String, partitions: String*): Unit = {
    spark.sqlContext.addPartitions(tableName, partitions: _*)
  }

  /**
   * 为指定表添加分区
   *
   * @param tableName
   * 表名
   * @param partition
   * 分区
   * @param partitionName
   * 分区字段名称，默认ds
   */
  def addPartition(tableName: String, partition: String, partitionName: String = GlobalConstants.SparkConf.partitionName): Unit = {
    spark.sqlContext.addPartition(tableName, partition, partitionName)
  }

  /**
   * 为指定表删除分区
   *
   * @param tableName
   * 表名
   * @param partition
   * 分区
   */
  def dropPartition(tableName: String, partition: String, partitionName: String = GlobalConstants.SparkConf.partitionName): Unit = {
    spark.sqlContext.dropPartition(tableName, partition, partitionName)
  }

  /**
   * 为指定表删除多个分区
   *
   * @param tableName
   * 表名
   * @param partitions
   * 分区
   */
  def dropPartitions(tableName: String, partitions: String*): Unit = {
    spark.sqlContext.dropPartitions(tableName, partitions: _*)
  }

  /**
   * 根据给定的表创建新表
   *
   * @param srcTableName
   * 源表
   * @param destTableName
   * 目标表
   */
  def createTableAsSelect(srcTableName: String, destTableName: String): Unit = {
    spark.sqlContext.createTableAsSelect(srcTableName, destTableName)
  }

  /**
   * 根据一张表创建另一张表
   *
   * @param tableName
   * 表名
   * @param destTableName
   * 目标表名
   */
  def createTableLike(tableName: String, destTableName: String): Unit = {
    spark.sqlContext.createTableLike(tableName, destTableName)
  }

  /**
   * 根据给定的表创建新表
   *
   * @param srcTableName
   * 来源表
   * @param destTableName
   * 目标表
   * @param cols
   * 多个列，逗号分隔
   */
  def createTableAsSelectFields(srcTableName: String, destTableName: String, cols: String): Unit = {
    spark.sqlContext.createTableAsSelectFields(srcTableName, destTableName, cols)
  }

  /**
   * 将数据插入到指定表的分区中
   *
   * @param srcTableName
   * 来源表
   * @param destTableName
   * 目标表
   * @param ds
   * 分区名
   * @param cols
   * 多个列，逗号分隔
   */
  def insertIntoPartition(srcTableName: String, destTableName: String, ds: String, cols: String, partitionName: String = GlobalConstants.SparkConf.partitionName): Unit = {
    spark.sqlContext.insertIntoPartition(srcTableName, destTableName, ds, cols, partitionName)
  }

  /**
   * 将sql执行结果插入到目标表指定分区中
   *
   * @param destTableName
   * 目标表名
   * @param ds
   * 分区名
   * @param querySQL
   * 查询语句
   */
  def insertIntoPartitionAsSelect(destTableName: String, ds: String, querySQL: String, partitionName: String = GlobalConstants.SparkConf.partitionName, overwrite: Boolean = false): Unit = {
    spark.sqlContext.insertIntoPartitionAsSelect(destTableName, ds, querySQL, partitionName, overwrite)
  }

  /**
   * 将sql执行结果插入到目标表指定分区中
   *
   * @param destTableName
   * 目标表名
   * @param querySQL
   * 查询sql语句
   */
  def insertIntoDymPartitionAsSelect(destTableName: String, querySQL: String, partitionName: String = GlobalConstants.SparkConf.partitionName): Unit = {
    spark.sqlContext.insertIntoDymPartitionAsSelect(destTableName, querySQL, partitionName)
  }

  /**
   * 构建Hive和HBase的映射表
   *
   * @param clazz
   * JavaBean的类型
   */
  def createHiveHBaseMappingTable[T <: HBaseBaseBean[T]](clazz: Class[T], tableName: String): Unit = {
    spark.sqlContext.createHiveHBaseMappingTable(clazz, tableName)
  }

  /**
   * 修改表名
   *
   * @param oldTableName
   * 表名称
   * @param newTableName
   * 新的表名
   */
  def rename(oldTableName: String, newTableName: String): Unit = {
    spark.sqlContext.rename(oldTableName, newTableName)
  }

  /**
   * 将表从一个db移动到另一个db中
   *
   * @param tableName
   * 表名
   * @param oldDB
   * 老库名称
   * @param newDB
   * 新库名称
   */
  def moveDB(tableName: String, oldDB: String, newDB: String): Unit = {
    spark.sqlContext.moveDB(tableName, oldDB, newDB)
  }

  // ----------------------------------- HBase Bulk API ----------------------------------- //

  /**
   * scan数据，并转为RDD
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T]): RDD[T] = {
    this.hbaseContext.bulkScanRDD(tableName, scan, clazz)
  }

  /**
   * scan数据，并转为RDD
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * 开始
   * @param stopRow
   * 结束
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T]): RDD[T] = {
    this.hbaseContext.bulkScanRDD(tableName, startRow, stopRow, clazz)
  }

  /**
   * 使用bulk方式scan数据，并转为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T]): DataFrame = {
    val rdd = this.hbaseContext.bulkScanRDD(tableName, scan, clazz)
    this.spark.createDataFrame(rdd, clazz)
  }

  /**
   * 使用bulk方式scan数据，并转为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * 开始
   * @param stopRow
   * 结束
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T]): DataFrame = {
    this.hbaseBulkScanDF[T](tableName, HBaseOper.buildScan(startRow, stopRow), clazz)
  }

  /**
   * 使用bulk方式scan数据，并转为Dataset
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T]): Dataset[T] = {
    val rdd = this.hbaseContext.bulkScanRDD(tableName, scan, clazz)
    this.spark.createDataset(rdd)(Encoders.bean(clazz))
  }

  /**
   * 使用bulk方式scan数据，并转为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * 开始
   * @param stopRow
   * 结束
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T]): Dataset[T] = {
    this.hbaseBulkScanDS[T](tableName, HBaseOper.buildScan(startRow, stopRow), clazz)
  }

  /**
   * 使用bulk方式批量插入数据
   *
   * @param tableName
   * HBase表名
   * 数据集合，继承自HBaseBaseBean
   * @param insertEmpty
   * 为空的字段是否写入
   */
  def hbaseBulkPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    rdd.hbaseBulkPutRDD(tableName, insertEmpty, multiVersion)
  }

  /** hbaseOperInsertList
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @param insertEmpty
   * 对象中值为空的字段是否覆盖HBase中已有的field值
   * 默认为覆盖
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def hbaseBulkPutDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    dataFrame.hbaseBulkPutDF[T](tableName, clazz, insertEmpty, multiVersion)
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @param dataset
   * dataFrame实例，数类型需继承自HBaseBaseBean
   * @param insertEmpty
   * 对象中值为空的字段是否覆盖HBase中已有的field值
   * 默认为覆盖
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def hbaseBulkPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataset: Dataset[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    dataset.hbaseBulkPutDS[T](tableName, insertEmpty, multiVersion)
  }

  /**
   * DStrea数据实时写入
   *
   * @param tableName
   * HBase表名
   */
  def hbaseBulkPutStream[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dstream: DStream[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    dstream.hbaseBulkPutStream[T](tableName, insertEmpty, multiVersion)
  }

  /**
   * 根据RDD[String]批量删除
   *
   * @param tableName
   * HBase表名
   * @param rowKeyRDD
   * 装有rowKey的rdd集合
   * @param batchSize
   * 批量删除的大小
   */
  def hbaseBulkDeleteRDD(tableName: String, rowKeyRDD: RDD[String], batchSize: Integer = this.hbaseContext.batchSize): Unit = {
    rowKeyRDD.hbaseBulkDeleteRDD(tableName, batchSize)
  }

  /**
   * 根据Dataset[String]批量删除，Dataset是rowkey的集合
   * 类型为String
   *
   * @param tableName
   * HBase表名
   * @param batchSize
   * 批量删除的大小，默认为1000条
   */
  def hbaseBulkDeleteDS(tableName: String, dataSet: Dataset[String], batchSize: Integer = this.hbaseContext.batchSize): Unit = {
    dataSet.hbaseBulkDeleteDS(tableName, batchSize)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   * 内部实现是将rowkey集合转为RDD[String]，推荐在数据量较大
   * 时使用。数据量较小请优先使用HBaseOper
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 具体类型
   * @param seq
   * rowKey集合
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def hbaseBulkGetSeq[E <: HBaseBaseBean[E] : ClassTag](tableName: String, seq: Seq[String], clazz: Class[E]): RDD[E] = {
    this.hbaseContext.bulkGetSeq[E](tableName, seq, clazz)
  }

  /**
   * 根据rowKey集合批量获取数据
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 获取后的记录转换为目标类型
   * @param batchSize
   * 批量的大小
   * @return
   * 结果集
   */
  def hbaseBulkGetRDD[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[E], batchSize: Integer = this.hbaseContext.batchSize): RDD[E] = {
    rowKeyRDD.hbaseBulkGetRDD[E](tableName, clazz, batchSize)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @param batchSize
   * 用于指定一次获取多少条记录，默认1000条
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def hbaseBulkGetDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[E], batchSize: Integer = this.hbaseContext.batchSize): DataFrame = {
    rowKeyRDD.hbaseBulkGetDF[E](tableName, clazz, batchSize)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @param batchSize
   * 用于指定一次获取多少条记录，默认1000条
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def hbaseBulkGetDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[E], batchSize: Integer = this.hbaseContext.batchSize): Dataset[E] = {
    rowKeyRDD.hbaseBulkGetDS[E](tableName, clazz, batchSize)
  }

  // ----------------------------------- HBase Spark API ----------------------------------- //

  /**
   * 使用Spark API的方式将RDD中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   */
  def hbaseHadoopPutRDD[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[E], insertEmpty: Boolean = true): Unit = {
    rdd.hbaseHadoopPutRDD(tableName, insertEmpty)
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   */
  def hbaseHadoopPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[E], insertEmpty: Boolean = true): Unit = {
    dataFrame.hbaseHadoopPutDF(tableName, clazz, insertEmpty)
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param dataset
   * JavaBean类型，待插入到hbase的数据集
   */
  def hbaseHadoopPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E], insertEmpty: Boolean = true): Unit = {
    dataset.hbaseHadoopPutDS[E](tableName, insertEmpty)
  }

  /**
   * 以spark 方式批量将DataFrame数据写入到hbase中
   *
   * @param tableName
   * hbase表名
   * @param insertEmpty
   * 为空的字段是否写入hbase
   * @tparam T
   * JavaBean类型
   */
  def hbaseHadoopPutDFRow[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataFrame: DataFrame, buildRowKey: (Row) => String, insertEmpty: Boolean = true): Unit = {
    dataFrame.hbaseHadoopPutDFRow[T](tableName, buildRowKey, insertEmpty)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * 目标类型
   * @return
   */
  def hbaseHadoopScanRS(tableName: String, scan: Scan, multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[(ImmutableBytesWritable, Result)] = {
    HBaseSparkBridge.hbaseHadoopScanRS(this.spark, tableName, scan, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * rowKey开始位置
   * @param stopRow
   * rowKey结束位置
   * 目标类型
   * @return
   */
  def hbaseHadoopScanRS2(tableName: String, startRow: String, stopRow: String, multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[(ImmutableBytesWritable, Result)] = {
    HBaseSparkBridge.hbaseHadoopScanRS2(spark, tableName, startRow, stopRow, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(T]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * 目标类型
   * @return
   */
  def hbaseHadoopScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    HBaseSparkBridge.hbaseHadoopScanRDD[T](spark, tableName, scan, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[T]
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * rowKey开始位置
   * @param stopRow
   * rowKey结束位置
   * 目标类型
   * @return
   */
  def hbaseHadoopScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    HBaseSparkBridge.hbaseHadoopScanRDD2[T](spark, tableName, startRow, stopRow, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(T]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * 目标类型
   * @return
   */
  def hbaseHadoopScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    HBaseSparkBridge.hbaseHadoopScanDF[T](this.spark, tableName, scan, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * rowKey开始位置
   * @param stopRow
   * rowKey结束位置
   * 目标类型
   * @return
   */
  def hbaseHadoopScanDF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    HBaseSparkBridge.hbaseHadoopScanDF2[T](this.spark, tableName, startRow, stopRow, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(T]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * 目标类型
   * @return
   */
  def hbaseHadoopScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    HBaseSparkBridge.hbaseHadoopScanDS[T](this.spark, tableName, scan, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * rowKey开始位置
   * @param stopRow
   * rowKey结束位置
   * 目标类型
   * @return
   */
  def hbaseHadoopScanDS2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    HBaseSparkBridge.hbaseHadoopScanDS2[T](this.spark, tableName, startRow, stopRow, clazz, multiVersion)
  }

  // ----------------------------------- HBase Oper API ----------------------------------- //

  /**
   * Scan指定HBase表的数据，并映射为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    HBaseSparkBridge.hbaseOperScanDF(this.spark, tableName, scan, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为DataFrame
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @param clazz
   *                目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperScanDF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    HBaseSparkBridge.hbaseOperScanDF2(this.spark, tableName, startRow, stopRow, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为Dataset
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    HBaseSparkBridge.hbaseOperScanDS[T](spark, tableName, scan, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为Dataset
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @param clazz
   *                目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperScanDS2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    HBaseSparkBridge.hbaseOperScanDS2[T](spark, tableName, startRow, stopRow, clazz, multiVersion, versions)
  }

  /**
   * 使用hbase java api方式插入一个集合的数据到hbase表中
   *
   * @param tableName
   * hbase表名
   * @param seq
   * HBaseBaseBean的子类集合
   * @param insertEmpty
   * 是否插入为空的字段
   * @param multiVersion
   * 是否以多版本形式插入
   */
  def hbaseOperPutList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    HBaseSparkBridge.hbaseOperPutList[T](tableName, seq, insertEmpty, multiVersion)
  }

  /**
   * 使用Java API的方式将RDD中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param batchSize
   * 批次大小
   * @param multiVersion
   * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
   */
  def hbaseOperPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    rdd.hbaseOperPutRDD[T](tableName, insertEmpty, batchSize, multiVersion)
  }

  /**
   * 使用Java API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param df
   * DataFrame
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   * @param batchSize
   * 批次大小
   * @param multiVersion
   * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
   */
  def hbaseOperPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, df: DataFrame, clazz: Class[E], insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    df.hbaseOperPutDF(tableName, clazz, insertEmpty, batchSize, multiVersion)
  }

  /**
   * 使用Java API的方式将Dataset中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   * @param batchSize
   * 批次大小
   * @param multiVersion
   * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
   */
  def hbaseOperPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E], clazz: Class[E], insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    dataset.hbaseOperPutDS[E](tableName, clazz, insertEmpty, batchSize, multiVersion)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * HBase scan对象
   * @return
   */
  def hbaseOperScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    HBaseSparkBridge.hbaseOperScanRDD(spark, tableName, scan, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * rowKey开始位置
   * @param stopRow
   * rowKey结束位置
   * 目标类型
   * @return
   */
  def hbaseOperScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    HBaseSparkBridge.hbaseOperScanRDD(spark, tableName, HBaseOper.buildScan(startRow, stopRow), clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为List
   *
   * @param tableName
   * HBase表名
   * @param scan
   * hbase scan对象
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperScanList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Seq[T] = {
    HBaseSparkBridge.hbaseOperScanList[T](tableName, scan, clazz, multiVersion, versions)
  }

  /**
   * Scan指定HBase表的数据，并映射为List
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @param clazz
   *                目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperScanList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE, operator: FilterList.Operator = null, filters: Filter = null): Seq[T] = {
    HBaseSparkBridge.hbaseOperScanList2[T](tableName, startRow, stopRow, clazz, multiVersion, versions)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 目标类型
   * @param multiVersion
   * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperGetRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    rdd.hbaseOperGetRDD(tableName, clazz, multiVersion, versions)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 目标类型
   * @param multiVersion
   * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperGetDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    rdd.hbaseOperGetDF(tableName, clazz, multiVersion, versions)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 目标类型
   * @param multiVersion
   * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseOperGetDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    rdd.hbaseOperGetDS[T](tableName, clazz, multiVersion, versions)
  }

  /**
   * 根据rowKey查询数据，并转为List[T]
   *
   * @param tableName
   * hbase表名
   * @param seq
   * rowKey集合
   * @param clazz
   * 目标类型
   * @param multiVersion
   * 是否get多版本
   * @param versions
   * get的版本数
   * @return
   * List[T]
   */
  def hbaseOperGetList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[Get], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Seq[T] = {
    HBaseSparkBridge.hbaseOperGetList[T](tableName, seq, clazz, multiVersion, versions)
  }

  /**
   * 根据rowKey查询数据，并转为List[T]
   *
   * @param tableName
   * hbase表名
   * @param seq
   * rowKey集合
   * @param clazz
   * 目标类型
   * @param multiVersion
   * 是否get多版本
   * @param versions
   * get的版本数
   * @return
   * List[T]
   */
  def hbaseOperGetList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Seq[T] = {
    HBaseSparkBridge.hbaseOperGetList2[T](tableName, seq, clazz, multiVersion, versions)
  }

  /**
   * 根据rowKey集合批量删除记录
   *
   * @param tableName
   * hbase表名
   * @param rowKeys
   * rowKey集合
   */
  def hbaseOperDeleteList(tableName: String, rowKeys: Seq[String]): Unit = {
    HBaseSparkBridge.hbaseOperDeleteList(tableName, rowKeys)
  }

  /**
   * 根据RDD[RowKey]批量删除记录
   *
   * @param tableName
   * rowKey集合
   * @param rowKeyRDD
   * rowKey的rdd集合
   * @param batchSize
   * 一次删除多少条
   */
  def hbaseOperDeleteRDD(tableName: String, rowKeyRDD: RDD[String], batchSize: Int = this.hbaseContext.batchSize): Unit = {
    rowKeyRDD.hbaseOperDeleteRDD(tableName, batchSize)
  }

  /**
   * 根据Dataset[RowKey]批量删除记录
   *
   * @param tableName
   * rowKey集合
   * @param batchSize
   * 一次删除多少条
   */
  def hbaseOperDeleteDS(tableName: String, dataset: Dataset[String], batchSize: Int = this.hbaseContext.batchSize): Unit = {
    dataset.hbaseOperDeleteDS(tableName, batchSize)
  }

  // ----------------------------------- Kafka 相关API ----------------------------------- //

  /**
   * 消费kafka中的json数据，并解析成json字符串
   *
   * @param extraOptions
   * 消费kafka额外的参数，如果有key同时出现在配置文件中和extraOptions中，将被extraOptions覆盖
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 转换成json字符串后的Dataset
   */
  def loadKafka(extraOptions: mutable.HashMap[String, String] = null, keyNum: Int = 1): Dataset[(String, String)] = {
    val groupId = if (StringUtils.isNotBlank(GlobalConstants.KafkaConf.kafkaGroupId(keyNum))) GlobalConstants.KafkaConf.kafkaGroupId(keyNum) else this.sc.appName
    val finalBrokers = GlobalConstants.KafkaConf.kafkaBrokers(keyNum)
    ValueUtils.requireNonNullForce(finalBrokers, s"kafka broker地址不能为空，可在配置文件中[ spark.kafka.brokers.name$keyNum ]指定")
    val kafkaReader = spark.readStream.format("kafka").option("group.id", groupId).option("kafka.bootstrap.servers", finalBrokers)
    val topics = GlobalConstants.KafkaConf.kafkaTopics()
    ValueUtils.requireNonNullForce(topics, s"kafka topic不能为空，可在配置文件中[ spark.kafka.topics$keyNum ]指定")
    kafkaReader.option("subscribe", topics)
    // 是否在数据丢失时失败
    val failOnDataLoss = GlobalConstants.KafkaConf.kafkaFailOnDataLoss(keyNum)
    if (failOnDataLoss != null) kafkaReader.option("failOnDataLoss", failOnDataLoss)
    // 指定起始消费位点
    val startingOffsets = GlobalConstants.KafkaConf.kafkaStartingOffset(keyNum)
    if (StringUtils.isNotBlank(startingOffsets)) kafkaReader.option("startingOffsets", startingOffsets)
    // 指定结束消费位点
    val endingOffsets = GlobalConstants.KafkaConf.kafkaEndingOffsets(keyNum)
    if (StringUtils.isNotBlank(endingOffsets)) kafkaReader.option("endingOffsets", endingOffsets)
    // 轮询数据的超时时间（以毫秒为单位）
    val pollTimeoutMs = GlobalConstants.KafkaConf.kafkaPollTimeoutMs(keyNum)
    if (pollTimeoutMs != null) kafkaReader.option("kafkaConsumer.pollTimeoutMs", pollTimeoutMs)
    // 放弃获取Kafka偏移前重试的次数
    val fetchOffsetNumRetries = GlobalConstants.KafkaConf.kafkaFetchOffsetNumRetries(keyNum)
    if (fetchOffsetNumRetries != null) kafkaReader.option("fetchOffset.numRetries", fetchOffsetNumRetries)
    // 重试获取Kafka偏移之前要等待的毫秒数
    val fetchOffsetRetryIntervalMs = GlobalConstants.KafkaConf.kafkaFetchOffsetRetryIntervalMs(keyNum)
    if (fetchOffsetRetryIntervalMs != null) kafkaReader.option("fetchOffset.retryIntervalMs", fetchOffsetRetryIntervalMs)
    // 每个触发间隔处理的最大偏移量的速率限制
    val maxOffsetsPerTrigger = GlobalConstants.KafkaConf.kafkaMaxOffsetsPerTrigger(keyNum)
    if (maxOffsetsPerTrigger > 0) kafkaReader.option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)

    // ------------------- kafka相关参数 ------------------- //
    // 心跳间隔时间
    val heartbeatInterval = GlobalConstants.KafkaConf.kafkaHeartbeatInterval(keyNum)
    if (heartbeatInterval > 0) {
      kafkaReader.option("kafka.heartbeat.interval.ms", heartbeatInterval.toString)
    }
    // 消费者组最大的session超时时间
    val groupMaxSessionTimeOut = GlobalConstants.KafkaConf.kafkaGroupMaxSessionTimeOut(keyNum)
    if (groupMaxSessionTimeOut > 0) {
      kafkaReader.option("kafka.group.max.session.timeout.ms", groupMaxSessionTimeOut.toString)
    }
    // 消费者组最小的session超时时间
    val groupMinSessionTimeOut = GlobalConstants.KafkaConf.kafkaGroupMinSessionTimeOut(keyNum)
    if (groupMinSessionTimeOut > 0) {
      kafkaReader.option("kafka.group.min.session.timeout.ms", groupMinSessionTimeOut.toString)
    }
    // 一次调用pool返回的最大记录数
    val maxPollRecords = GlobalConstants.KafkaConf.kafkaMaxPollRecords(keyNum)
    if (maxPollRecords > 0) {
      kafkaReader.option("kafka.max.poll.records", maxPollRecords.toString)
    }
    // 每个分区返回的最大数据量
    val maxPartitionFetchBytes = GlobalConstants.KafkaConf.kafkaMaxPartitionFetchBytes(keyNum)
    if (maxPartitionFetchBytes > 0) {
      kafkaReader.option("kafka.max.partition.fetch.bytes", maxPartitionFetchBytes.toString)
    }

    // 用户指定参数
    if (extraOptions != null && extraOptions.size > 0) kafkaReader.options(extraOptions)

    kafkaReader.load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as value").as[(String, String)]
  }

  /**
   * 消费kafka中的json数据，并按照指定的schema解析成目标类型
   *
   * @param schemaClass
   * json对应的javabean类型
   * @param extraOptions
   * 消费kafka额外的参数
   * @param parseAll
   * 是否解析所有字段信息
   * @param isMySQL
   * 是否为mysql解析的消息
   * @param fieldNameUpper
   * 字段名称是否为大写
   * @return
   * 转换成json字符串后的Dataset
   */
  def loadKafkaParse(schemaClass: Class[_],
                     extraOptions: mutable.HashMap[String, String] = null,
                     parseAll: Boolean = false,
                     isMySQL: Boolean = true,
                     fieldNameUpper: Boolean = false, keyNum: Int = 1): DataFrame = {
    val kafkaDataset = this.loadKafka(extraOptions, keyNum)
    val schemaDataset = kafkaDataset.select(from_json($"value", SparkUtils.buildSchema2Kafka(schemaClass, parseAll, isMySQL, fieldNameUpper)).as("data"))
    if (parseAll)
      schemaDataset.select("data.*")
    else
      schemaDataset.select("data.after.*")
  }

  /**
   * 消费kafka中的json数据，并自动解析json数据，将解析后的数据注册到tableName所指定的临时表中
   *
   * @param tableName
   * 解析后的数据存放的临时表名，默认名为kafka
   * @param extraOptions
   * 消费kafka额外的参数
   * @return
   * 转换成json字符串后的Dataset
   */
  def loadKafkaParseJson(tableName: String = "kafka",
                         extraOptions: mutable.HashMap[String, String] = null,
                         keyNum: Int = 1): DataFrame = {
    val msg = KafkaUtils.getMsg(GlobalConstants.KafkaConf.kafkaBrokers(keyNum), GlobalConstants.KafkaConf.kafkaTopics(keyNum), null)
    ValueUtils.requireNonNullForce(msg, s"获取样例消息失败！请重启任务尝试重新获取，并保证topic[${GlobalConstants.KafkaConf.kafkaTopics(keyNum)}]持续的有新消息。")
    val jsonDS = this.spark.createDataset(Seq(msg))(Encoders.STRING)
    val jsonDF = this.spark.read.json(jsonDS)

    val kafkaDataset = this.loadKafka(extraOptions, keyNum)
    val schemaDataset = kafkaDataset.select(from_json($"value", jsonDF.schema).as(tableName)).select(s"${tableName}.*")
    schemaDataset.createOrReplaceTempView(tableName)
    schemaDataset
  }

  /**
   * 解析DStream中每个rdd的json数据，并转为DataFrame类型
   *
   * @param schema
   * 目标DataFrame类型的schema
   * @param isMySQL
   * 是否为mysql解析的消息
   * @param fieldNameUpper
   * 字段名称是否为大写
   * @param parseAll
   * 是否需要解析所有字段信息
   * @return
   */
  def kafkaJson2DFV(rdd: RDD[String], schema: Class[_], parseAll: Boolean = false, isMySQL: Boolean = true, fieldNameUpper: Boolean = false): DataFrame = {
    rdd.kafkaJson2DFV(schema, parseAll, isMySQL, fieldNameUpper)
  }

  /**
   * 解析DStream中每个rdd的json数据，并转为DataFrame类型
   *
   * @param schema
   * 目标DataFrame类型的schema
   * @param isMySQL
   * 是否为mysql解析的消息
   * @param fieldNameUpper
   * 字段名称是否为大写
   * @param parseAll
   * 是否解析所有字段信息
   * @return
   */
  def kafkaJson2DF(rdd: RDD[ConsumerRecord[String, String]], schema: Class[_], parseAll: Boolean = false, isMySQL: Boolean = true, fieldNameUpper: Boolean = false): DataFrame = {
    rdd.kafkaJson2DF(schema, parseAll, isMySQL, fieldNameUpper)
  }

  /**
   * 清理 RDD、DataFrame、Dataset、DStream、TableName 缓存
   * 等同于uncache
   *
   * @param any
   * RDD、DataFrame、Dataset、DStream、TableName
   */
  def unpersist(any: Any*): Unit = {
    this.uncache(any: _*)
  }

  /**
   * 清理 RDD、DataFrame、Dataset、DStream、TableName 缓存
   * 等同于unpersist
   *
   * @param any
   * RDD、DataFrame、Dataset、DStream、TableName
   */
  def uncache(any: Any*): Unit = {
    if (any != null && any.size > 0) {
      any.foreach(elem => {
        if (elem != null) {
          if (elem.isInstanceOf[String]) {
            val tableName = elem.asInstanceOf[String]
            if (this.tableExists(tableName) && this.isCached(tableName)) {
              this.spark.sqlContext.uncacheTables(tableName)
            }
          } else if (elem.isInstanceOf[Dataset[_]]) {
            elem.asInstanceOf[Dataset[_]].uncache
          } else if (elem.isInstanceOf[DataFrame]) {
            elem.asInstanceOf[DataFrame].uncache
          } else if (elem.isInstanceOf[RDD[_]]) {
            elem.asInstanceOf[RDD[_]].uncache
          } else if (elem.isInstanceOf[DStream[_]]) {
            elem.asInstanceOf[DStream[_]].uncache
          }
        }
      })
    }
  }

  // ----------------------------------- 关系型数据库API ----------------------------------- //

  /**
   * 关系型数据库插入、删除、更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param params
   * sql中的参数
   * @param connection
   * 传递已有的数据库连接
   * @param commit
   * 是否自动提交事务，默认为自动提交
   * @param closeConnection
   * 是否关闭connection，默认关闭
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 影响的记录数
   */
  def jdbcUpdate(sql: String, params: Seq[Any] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Long = {
    JdbcOper.executeUpdate(sql, params, connection, commit, closeConnection, keyNum)
  }

  /**
   * 关系型数据库批量插入、删除、更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param paramsList
   * sql的参数列表
   * @param connection
   * 传递已有的数据库连接
   * @param commit
   * 是否自动提交事务，默认为自动提交
   * @param closeConnection
   * 是否关闭connection，默认关闭
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 影响的记录数
   */
  def jdbcBatchUpdate(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Array[Int] = {
    JdbcOper.executeBatch(sql, paramsList, connection, commit, closeConnection, keyNum)
  }

  /**
   * 执行查询操作，以JavaBean方式返回结果集
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param clazz
   * JavaBean类型
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 查询结果集
   */
  def jdbcQuery[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, clazz: Class[T], connection: Connection = null, keyNum: Int = 1): List[T] = {
    JdbcOper.executeQuery[T](sql, params, clazz, connection, keyNum)
  }

  /**
   * 执行查询操作，以RDD方式返回结果集
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param clazz
   * JavaBean类型
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return 查询结果集
   */
  def jdbcQueryRDD[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, clazz: Class[T], connection: Connection = null, keyNum: Int = 1): RDD[T] = {
    val rsList = JdbcOper.executeQuery[T](sql, params, clazz, connection, keyNum)
    this.sc.parallelize(rsList, FireConf.jdbcQueryPartitions).persist(FireConf.jdbcStorageLevel)
  }

  /**
   * 执行查询操作，以DataFrame方式返回结果集
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param clazz
   * JavaBean类型
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return 查询结果集
   */
  def jdbcQueryDF[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, clazz: Class[T], connection: Connection = null, keyNum: Int = 1): DataFrame = {
    this.spark.createDataFrame(this.jdbcQueryRDD(sql, params, clazz, connection, keyNum), clazz)
  }

  /**
   * 执行查询操作，以Dataset方式返回结果集
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param clazz
   * JavaBean类型
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 查询结果集
   */
  def jdbcQueryDS[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, clazz: Class[T], connection: Connection = null, keyNum: Int = 1): Dataset[T] = {
    this.spark.createDataset[T](this.jdbcQueryRDD(sql, params, clazz, connection, keyNum))(Encoders.bean(clazz))
  }

  /**
   * 执行查询操作，并在QueryCallback对结果集进行处理
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param callback
   * 查询回调
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   */
  def jdbcQueryCall(sql: String, params: Seq[Any] = null, callback: QueryCallback = null, connection: Connection = null, keyNum: Int = 1): Unit = {
    JdbcOper.executeQueryCall(sql, params, callback, connection, keyNum)
  }

  /**
   * 将DataFrame数据保存到关系型数据库中
   *
   * @param dataFrame
   * DataFrame数据集
   * @param tableName
   * 关系型数据库表名
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   */
  def jdbcTableSave(dataFrame: DataFrame, tableName: String, saveMode: SaveMode = SaveMode.Append, jdbcProps: Properties = null, keyNum: Int = 1): Unit = {
    dataFrame.jdbcTableSave(tableName, saveMode, jdbcProps, keyNum)
  }

  /**
   * 单线程加载一张关系型数据库表
   * 注：仅限用于小的表，不支持条件查询
   *
   * @param tableName
   * 关系型数据库表名
   * @param jdbcProps
   * 调用者指定的数据库连接信息，如果为空，则默认读取配置文件
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * DataFrame
   */
  def jdbcTableLoadAll(tableName: String, jdbcProps: Properties = null, keyNum: Int = 1): DataFrame = {
    this.spark.sqlContext.jdbcTableLoadAll(tableName, jdbcProps, keyNum)
  }

  /**
   * 指定load的条件，从关系型数据库中并行的load数据，并转为DataFrame
   *
   * @param tableName 数据库表名
   * @param predicates
   *                  并行load数据时，每一个分区load数据的where条件
   *                  比如：gmt_create >= '2019-06-20' AND gmt_create <= '2019-06-21' 和 gmt_create >= '2019-06-22' AND gmt_create <= '2019-06-23'
   *                  那么将两个线程同步load，线程数与predicates中指定的参数个数保持一致
   * @param keyNum
   *                  配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   *                  比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 查询结果集
   */
  def jdbcTableLoad(tableName: String, predicates: Array[String], jdbcProps: Properties = null, keyNum: Int = 1): DataFrame = {
    this.spark.sqlContext.jdbcTableLoad(tableName, predicates, jdbcProps, keyNum)
  }

  /**
   * 根据指定字段的范围load关系型数据库中的数据
   *
   * @param tableName
   * 表名
   * @param columnName
   * 表的分区字段
   * @param lowerBound
   * 分区的下边界
   * @param upperBound
   * 分区的上边界
   * @param jdbcProps
   * jdbc连接信息，默认读取配置文件
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   */
  def jdbcTableLoadBound(tableName: String, columnName: String, lowerBound: Long, upperBound: Long, numPartitions: Int = 10, jdbcProps: Properties = null, keyNum: Int = 1): DataFrame = {
    this.spark.sqlContext.jdbcTableLoadBound(tableName, columnName, lowerBound, upperBound, keyNum, jdbcProps, keyNum)
  }

  /**
   * 将DataFrame中指定的列写入到jdbc中
   * 调用者需自己保证DataFrame中的列类型与关系型数据库对应字段类型一致
   *
   * @param dataFrame
   * 将要插入到关系型数据库中原始的数据集
   * @param sql
   * 关系型数据库待执行的增删改sql
   * @param fields
   * 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序
   * 若不指定字段，则默认传入当前DataFrame所有列，且列的顺序与sql中问号占位符顺序一致
   * @param batch
   * 每个批次执行多少条
   * @param keyNum
   * 对应配置文件中指定的数据源编号
   */
  def jdbcBatchUpdateDF(dataFrame: DataFrame, sql: String, fields: Seq[String] = null, batch: Int = GlobalConstants.JdbcConf.batchSize(), keyNum: Int = 1): Unit = {
    if (ValueUtils.isEmpty(dataFrame)) {
      this.log("执行jdbcBatchUpdateDF失败，dataFrame或sql为空")
      return
    }
    dataFrame.jdbcBatchUpdate(sql, fields, batch, keyNum)
  }
}