package com.zto.bigdata.spark.common.ext

import com.zto.bigdata.spark.common.bean.HBaseBaseBean
import com.zto.bigdata.spark.common.db.{HBaseOper, HBaseSparkBridge}
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.udf.UDFs
import com.zto.bigdata.spark.common.util._
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.from_json

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * SparkContext扩展
  *
  * @param spark
  * sparkSession对象
  * @author ChengLong 2019-5-18 10:51:19
  */
class SparkSessionExt(spark: SparkSession) {

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
    * load关系型数据库整张表。若load部分数据，请使用：loadDBToBean()
    *
    * @param table
    * 表名
    * @return
    * DataFrame
    */
  def loadDBTable(table: String): DataFrame = {
    spark.sqlContext.loadDBTable(table)
  }

  /**
    * 从oracle表中load数据
    *
    * @param tableName
    * 表名
    * @param predicates
    * 配置信息
    * @return
    * DataFrame
    */
  def loadOracleData(tableName: String, predicates: Array[String]): DataFrame = {
    spark.sqlContext.loadOracleData(tableName, predicates)
  }

  /**
    * 批量清空多张缓存表
    *
    * @param tables
    * 多个表名
    */
  def uncacheTables(tables: String*): Unit = {
    spark.sqlContext.uncacheTables(tables: _*)
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

  /**
    * 消费kafka中的json数据，并解析成json字符串
    *
    * @param brokers
    * brokers地址
    * @param extraOptions
    * 消费kafka额外的参数
    * @return
    * 转换成json字符串后的Dataset
    */
  def loadKafka(brokers: String, extraOptions: mutable.HashMap[String, String]): Dataset[(String, String)] = {
    val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).options(extraOptions).load()
    kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as value").as[(String, String)]
  }

  /**
    * 消费kafka中的json数据，并解析成目标类型
    *
    * @param schemaClass
    * json对应的javabean类型
    * @param brokers
    * brokers地址
    * @param extraOptions
    * 消费kafka额外的参数
    * @param requireBefore
    * 是否解析before信息
    * @return
    * 转换成json字符串后的Dataset
    */
  def loadKafkaParseJson(schemaClass: Class[_],
                         brokers: String = GlobalConstants.SparkConf.kafkaBrokers,
                         extraOptions: mutable.HashMap[String, String] = mutable.HashMap[String, String]("subscribe" -> GlobalConstants.SparkConf.kafkaTopics, "failOnDataLoss" -> GlobalConstants.SparkConf.kafkaFailOnDataLoss.toString, "startingOffsets" -> GlobalConstants.SparkConf.kafkaStartingOffset, "enable.auto.commit" -> GlobalConstants.SparkConf.kafkaEnableAutoCommit.toString),
                         requireBefore: Boolean = false): DataFrame = {
    ParamUtils.requireNonNullForce(brokers, "kafka broker地址不能为空，可在配置文件中[ spark.kafka.brokers.url ]指定")
    ParamUtils.requireNonNullForce(extraOptions, "kafka extraOptions不能为空")
    ParamUtils.requireNonNullForce(extraOptions.getOrElse("subscribe", null), "topic不能为空，可在配置文件中[ spark.kafka.topics ]指定")

    val kafkaDataset = this.loadKafka(brokers, extraOptions)
    val schemaDataset = kafkaDataset.select(from_json($"value", SparkUtils.buildSchema2Kafka(schemaClass, requireBefore)).as("data"))
    if (requireBefore)
      schemaDataset.select("data.*")
    else
      schemaDataset.select("data.after.*")
  }

  /**
    * 根据指定的javabean，构建Streaming类型的carbondata表
    *
    * @param tableName
    * 表名
    * @param tableSchema
    * 表的schema信息，与javabean对应
    * @return
    * DataFrame
    */
  def createCarbonStreamingTable(dbName: String, tableName: String, tableSchema: Class[_]): DataFrame = {
    spark.sql(CarbondataUtils.buildCreateStreamingTableSQL(dbName, tableName, tableSchema))
  }

  /**
    * 根据指定的javabean，构建carbondata的分区表sql
    *
    * @param tableName
    * 表名
    * @param tableSchema
    * 表的schema信息，与javabean对应
    * @return
    * DataFrame
    */
  def createCarbonTable(dbName: String, tableName: String, tableSchema: Class[_], partition: String = GlobalConstants.SparkConf.partitionName): DataFrame = {
    spark.sql(CarbondataUtils.buildCreatePartitioinTableSQL(dbName, tableName, tableSchema, partition))
  }

  /**
    * 构建drop表的语句
    *
    * @param dbName
    * 数据库名
    * @param tableName
    * 表名
    */
  def dropCarbonTable(dbName: String = GlobalConstants.SparkConf.defaultDB, tableName: String): Unit = {
    spark.sql(CarbondataUtils.dropCarbonTable(dbName, tableName))
  }

  /**
    * 对指定的表执行minor compact
    *
    * @param dbName
    * 数据库名
    * @param tableName
    * 表名
    */
  def minorCompact(dbName: String = GlobalConstants.SparkConf.defaultDB, tableName: String): Unit = {
    spark.sql(CarbondataUtils.minorCompact(dbName, tableName))
  }

  /**
    * 对指定的表执行minor major
    *
    * @param dbName
    * 数据库名
    * @param tableName
    * 表名
    */
  def majorCompact(dbName: String = GlobalConstants.SparkConf.defaultDB, tableName: String): Unit = {
    spark.sql(CarbondataUtils.majorCompact(dbName, tableName))
  }

  /**
    * 将普通的carbondata表转换为streaming表
    *
    * @param dbName
    * 数据库名
    * @param tableName
    * 表名
    */
  def enableStreamingTable(dbName: String = GlobalConstants.SparkConf.defaultDB, tableName: String): Unit = {
    spark.sql(CarbondataUtils.enableStreamingTable(dbName, tableName))
  }

  /**
    * 批量注册自定义udf函数
    *
    * @return
    */
  def registerAll(): SparkSession = {
    UDFs.registerAll(spark)
    spark
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
  def hbaseBulkPut[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    rdd.hbaseBulkPut(tableName, insertEmpty, multiVersion)
  }

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
  def hbaseBulkScan[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T]): RDD[T] = {
    this.hbaseContext.bulkScan(tableName, scan, clazz)
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
  def hbaseBulkScan[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T]): RDD[T] = {
    this.hbaseContext.bulkScan(tableName, startRow, stopRow, clazz)
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
    val rdd = this.hbaseContext.bulkScan(tableName, scan, clazz)
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
    this.hbaseBulkScanDF[T](tableName, HBaseOper.buildScan(startRow, stopRow, null), clazz)
  }

  /**
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
  def hbaseBulkPutDataset[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataset: Dataset[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    dataset.hbaseBulkPutDataset[T](tableName, insertEmpty, multiVersion)
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
  def hbaseBulkDelete(tableName: String, rowKeyRDD: RDD[String], batchSize: Integer = this.hbaseContext.batchSize): Unit = {
    rowKeyRDD.hbaseBulkDelete(tableName, batchSize)
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
  def hbaseBulkGet[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[E], batchSize: Integer = this.hbaseContext.batchSize): RDD[HBaseBaseBean[E]] = {
    rowKeyRDD.hbaseBulkGet[E](tableName, clazz, batchSize)
  }

  /**
    * 使用Spark API的方式将RDD中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    */
  def hbaseHadoopPut[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[E], insertEmpty: Boolean = true): Unit = {
    rdd.hbaseHadoopPut(tableName, insertEmpty)
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
  def hbaseHadoopPutDataset[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E], insertEmpty: Boolean = true): Unit = {
    dataset.hbaseHadoopPutDataset[E](tableName, insertEmpty)
  }








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
  def hbaseScan2DF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    HBaseSparkBridge.hbaseScan2DF(this.spark, tableName, scan, clazz, multiVersion, versions)
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
  def hbaseScan2DF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    HBaseSparkBridge.hbaseScan2DF2(this.spark, tableName, startRow, stopRow, clazz, multiVersion, versions)
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
  def hbaseInsertDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, df: DataFrame, clazz: Class[E], insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    HBaseSparkBridge.hbaseInsertDF(tableName, df, clazz, insertEmpty, batchSize, multiVersion)
  }

  /**
    * 使用Java API的方式将RDD中的数据分多个批次插入到HBase中
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
  def hbaseInsertRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[_], clazz: Class[T], insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    HBaseSparkBridge.hbaseInsertRDD(tableName, rdd, clazz, insertEmpty, batchSize, multiVersion)
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
  def hbaseScan2HBaseRDD(tableName: String, scan: Scan, multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[(ImmutableBytesWritable, Result)] = {
    HBaseSparkBridge.hbaseScan2HBaseRDD(this.spark, tableName, scan, multiVersion, versions)
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
  def hbaseScan2HBaseRDD2(tableName: String, startRow: String, stopRow: String, multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[(ImmutableBytesWritable, Result)] = {
    HBaseSparkBridge.hbaseScan2HBaseRDD2(spark, tableName, startRow, stopRow, multiVersion, versions)
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
  def hbaseScan2RDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    HBaseSparkBridge.hbaseScan2RDD(spark, tableName, scan, clazz, multiVersion, versions)
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
  def hbaseScan2RDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    HBaseSparkBridge.hbaseScan2RDD(spark, tableName, HBaseOper.buildScan(startRow, stopRow, null), clazz, multiVersion, versions)
  }

  /**
    * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
    *
    * @param rowKeyRDD
    * rdd中存放了待查询的rowKey集合
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
  def hbaseGet2RDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    HBaseSparkBridge.hbaseGet2RDD(tableName, rowKeyRDD, clazz, multiVersion, versions)
  }

  /**
    * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
    *
    * @param rowKeyRDD
    * rdd中存放了待查询的rowKey集合
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
  def hbaseGet2DF[T <: HBaseBaseBean[T] : ClassTag](rowKeyRDD: RDD[String], tableName: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    HBaseSparkBridge.hbaseGet2DF(tableName, rowKeyRDD, clazz, multiVersion, versions)
  }

}