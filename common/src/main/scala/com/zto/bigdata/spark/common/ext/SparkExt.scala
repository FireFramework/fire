package com.zto.bigdata.spark.common.ext

import java.sql.DriverManager
import java.util.{Objects, Properties}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.parser.ParserConfig
import com.zto.bigdata.spark.common.acc.{MultiAccumulators, MultiDateTimeAccumulators}
import com.zto.bigdata.spark.common.bean.{HBaseBaseBean, OGGBaseBean}
import com.zto.bigdata.spark.common.db.{HBaseOper, HBaseSparkBridge}
import com.zto.bigdata.spark.common.udf.UDFs
import com.zto.bigdata.spark.common.util._
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.collection.{JavaConversions, mutable}
import scala.reflect._

/**
  * Spark扩展工具类，利用隐式转换对已有的类追加自定义函数
  * Created by ChengLong on 2017-02-07.
  */
object SparkExt {

  /**
    * SparkContext扩展
    *
    * @param spark
    */
  implicit class SparkSessionExt(spark: SparkSession) {

    import spark.implicits._

    // 获取单例的HBaseContext对象
    lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(spark.sparkContext)
    val sc: SparkContext = spark.sparkContext

    /**
      * 根据给定的集合，创建rdd
      *
      * @param seq
      * @param numSlices
      * @tparam T
      * @return
      */
    def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
      this.sc.parallelize(seq, numSlices)
    }

    /**
      * 执行一段Hive QL语句，注册为临时表，持久化到hive中
      *
      * @param sqlStr
      * @param tmpTableName
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
      * @param tmpTableName
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
      * @param tmpTableName
      * @return
      * 生成的DataFrame
      */
    def sqlNoCache(sqlStr: String, tmpTableName: String): DataFrame = {
      spark.sqlContext.sqlNoCache(sqlStr, tmpTableName)
    }

    /**
      * load关系型数据库整张表。若load部分数据，请使用：loadDBToBean()
      *
      * @return
      */
    def loadDBTable(table: String): DataFrame = {
      spark.sqlContext.loadDBTable(table)
    }

    /**
      * 从oracle表中load数据
      *
      * @param tableName
      * @param predicates
      * @return
      */
    def loadOracleData(tableName: String, predicates: Array[String]): DataFrame = {
      spark.sqlContext.loadOracleData(tableName, predicates)
    }

    /**
      * 批量清空多张缓存表
      *
      * @param tables
      */
    def uncacheTables(tables: String*) = {
      spark.sqlContext.uncacheTables(tables: _*)
    }

    /**
      * 批量缓存多张表
      *
      * @param tables
      */
    def cacheTables(tables: String*) = {
      spark.sqlContext.cacheTables(tables: _*)
    }

    /**
      * 删除指定的hive表
      *
      * @param tableNames
      */
    def dropHiveTable(tableNames: String*) = {
      spark.sqlContext.dropHiveTable(tableNames: _*)
    }

    /**
      * 为指定表添加分区
      *
      * @param tableName
      * 表名
      * @param partitions
      * 分区
      * @return
      */
    def addPartitions(tableName: String, partitions: String*) = {
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
      * @return
      */
    def addPartition(tableName: String, partition: String, partitionName: String = GlobalConstants.SparkConf.partitionName) = {
      spark.sqlContext.addPartition(tableName, partition, partitionName)
    }

    /**
      * 为指定表删除分区
      *
      * @param tableName
      * 表名
      * @param partition
      * 分区
      * @return
      */
    def dropPartition(tableName: String, partition: String, partitionName: String = GlobalConstants.SparkConf.partitionName) = {
      spark.sqlContext.dropPartition(tableName, partition, partitionName)
    }

    /**
      * 为指定表删除多个分区
      *
      * @param tableName
      * 表名
      * @param partitions
      * 分区
      * @return
      */
    def dropPartitions(tableName: String, partitions: String*) = {
      spark.sqlContext.dropPartitions(tableName, partitions: _*)
    }

    /**
      * 根据给定的表创建新表
      *
      * @param srcTableName
      * @param destTableName
      * @return
      */
    def createTableAsSelect(srcTableName: String, destTableName: String) = {
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
    def createTableLike(tableName: String, destTableName: String) = {
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
      * @return
      */
    def createTableAsSelectFields(srcTableName: String, destTableName: String, cols: String) = {
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
      * @return
      */
    def insertIntoPartition(srcTableName: String, destTableName: String, ds: String, cols: String, partitionName: String = GlobalConstants.SparkConf.partitionName) = {
      spark.sqlContext.insertIntoPartition(srcTableName, destTableName, ds, cols, partitionName)
    }

    /**
      * 将sql执行结果插入到目标表指定分区中
      *
      * @param destTableName
      * @param ds
      * @param querySQL
      * @return
      */
    def insertIntoPartitionAsSelect(destTableName: String, ds: String, querySQL: String, partitionName: String = GlobalConstants.SparkConf.partitionName, overwrite: Boolean = false) = {
      spark.sqlContext.insertIntoPartitionAsSelect(destTableName, ds, querySQL, partitionName, overwrite)
    }

    /**
      * 将sql执行结果插入到目标表指定分区中
      *
      * @param destTableName
      * @param querySQL
      * @return
      */
    def insertIntoDymPartitionAsSelect(destTableName: String, querySQL: String, partitionName: String = GlobalConstants.SparkConf.partitionName) = {
      spark.sqlContext.insertIntoDymPartitionAsSelect(destTableName, querySQL, partitionName)
    }

    /**
      * 构建Hive和HBase的映射表
      *
      * @param clazz
      */
    def createHiveHBaseMappingTable[T <: HBaseBaseBean[T]](clazz: Class[T], tableName: String) = {
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
      * @param tableName
      * @return
      */
    def minorCompact(dbName: String = GlobalConstants.SparkConf.defaultDB, tableName: String): Unit = {
      spark.sql(CarbondataUtils.minorCompact(dbName, tableName))
    }

    /**
      * 对指定的表执行minor major
      *
      * @param dbName
      * @param tableName
      * @return
      */
    def majorCompact(dbName: String = GlobalConstants.SparkConf.defaultDB, tableName: String): Unit = {
      spark.sql(CarbondataUtils.majorCompact(dbName, tableName))
    }

    /**
      * 将普通的carbondata表转换为streaming表
      *
      * @param dbName
      * @param tableName
      * @return
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
      * scan数据，并转为RDD
      *
      * @param tableName
      * HBase表名
      * @param scan
      * scan对象
      * @param clazz
      * @return
      * 结果集
      */
    def hbaseRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T]): RDD[T] = {
      this.hbaseContext.hbaseRDD(tableName, scan, clazz)
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
      * @return
      */
    def hbaseRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T]): RDD[T] = {
      this.hbaseContext.hbaseRDD(tableName, startRow, stopRow, clazz)
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
    def hbaseScan2DF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T]): DataFrame = {
      HBaseSparkBridge.hbaseScan2DF(this.spark, tableName, scan, clazz)
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
    def hbaseScan2DF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T]): DataFrame = {
      HBaseSparkBridge.hbaseScan2DF(this.spark, tableName, startRow, stopRow, clazz)
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
      */
    def hbaseInsertDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, df: DataFrame, clazz: Class[E], batchSize: Int = HBaseSparkBridge.batchSize): Unit = {
      HBaseSparkBridge.hbaseInsertDF(tableName, df, clazz, batchSize)
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
      */
    def hbaseInsertRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[_], clazz: Class[T], batchSize: Int = HBaseSparkBridge.batchSize): Unit = {
      HBaseSparkBridge.hbaseInsertRDD(tableName, rdd, clazz, batchSize)
    }
  }

  /**
    * SparkContext扩展
    *
    * @param sc
    */
  implicit class SparkContextExt(sc: SparkContext) {
    // 获取单例的HBaseContext对象
    private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(sc)

    /**
      * 根据多个key创建一个含有多个值的自定义多值累加器
      */
    def multiAccumulators(): Accumulator[collection.mutable.LinkedHashMap[String, Long]] = {
      val map = scala.collection.mutable.LinkedHashMap[String, Long]()
      this.sc.accumulator(map)(MultiAccumulators)
    }

    /**
      * 根据多个key创建一个含有多个值多个时间的自定义多值累加器
      */
    def multiDateTimeAccumulators: Accumulator[collection.mutable.Map[String, Long]] = {
      val map = scala.collection.mutable.Map[String, Long]()
      this.sc.accumulator(map)(MultiDateTimeAccumulators)
    }

    /**
      * 根据运行模式创建SQLContext或HiveContext
      *
      * @return
      */
    def createSQLContext: SQLContext = {
      if (GlobalConstants.isCluster) {
        new HiveContext(sc) //.set("hive.exec.compress.output", "true")
          .set("hive.exec.dynamic.partition", "true")
          .set("hive.exec.dynamic.partition.mode", "nonstrict")
          .set("hive.exec.max.dynamic.partitions", "1000")
          .set("hive.exec.max.dynamic.partitions.pernode", "1000")
          .set("hive.exec.compress.output", "true").set("mapred.output.compress", "true")
          .set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
          .set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec")
      } else {
        new SQLContext(sc)
      }
    }

    /**
      * 设置日志级别
      *
      * @return
      */
    def setLogLevel2: SparkContext = {
      val logLevel = if (StringUtils.isNotBlank(GlobalConstants.SparkConf.logLevel)) GlobalConstants.SparkConf.logLevel else "DEBUG"
      sc.setLogLevel(logLevel)
      sc
    }

    /**
      * 从关系型数据库中load数据后组装为RDD[bean]
      *
      * @param sql
      * @param day1
      * @param day2
      * @param numPartitions
      * @return
      */
    def loadDBToBean[T: ClassManifest](sql: String, day1: String, day2: String, clazz: Class[T], numPartitions: Int = GlobalConstants.SparkConf.parallelism): RDD[T] = {
      val lowerBound = DateFormatUtils.formatDateTime(day1).getTime / 1000
      val upperBound = DateFormatUtils.formatDateTime(day2).getTime / 1000
      new JdbcRDD(
        sc,
        () => {
          Class.forName(GlobalConstants.driverClass).newInstance()
          DriverManager.getConnection(GlobalConstants.rdburl, GlobalConstants.user, GlobalConstants.password)
        },
        sql,
        lowerBound, upperBound, numPartitions,
        row => SparkUtils.dbRow2Bean(row, clazz))
    }

    /**
      * 定义多个Long类型累加器
      *
      * @return
      */
    def defineLongAccumulators(accNames: String*): Map[String, Accumulator[Long]] = {
      var accMap = Map[String, Accumulator[Long]]()
      accNames.foreach(accName => {
        accMap += (accName -> sc.accumulator[Long](0L))
      })
      accMap
    }

  }


  /**
    * HBaseContext相关扩展
    *
    * @param rdd
    * @tparam T
    */
  implicit class RDDHBaseExt[T <: HBaseBaseBean[T] : ClassTag](rdd: RDD[T]) {
    // 获取单例的HBaseContext对象
    private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(rdd.sparkContext)

    /**
      * 批量插入数据
      *
      * @param tableName
      * HBase表名
      * 数据集合，继承自HBaseBaseBean
      * @param insertEmpty
      * 为空的字段是否写入
      */
    def hbaseBulkPut(tableName: String, insertEmpty: Boolean = true): Unit = {
      this.hbaseContext.bulkPut(tableName, rdd, insertEmpty)
    }

    /**
      * 批量插入多个历史版本的数据
      *
      * @param tableName
      * HBase表名
      * 数据集合，继承自HBaseBaseBean
      * @param insertEmpty
      * 为空的字段是否写入
      */
    def hbaseBulkPutMultiVersions(tableName: String, insertEmpty: Boolean = true): Unit = {
      this.hbaseContext.bulkPutMultiVersions(tableName, rdd, insertEmpty)
    }

    /*
        /**
          * 批量load数据到hbase
          *
          * @param tableName
          * HBase表名
          * @param stagingDir
          * 临时路径
          * @param insertEmpty
          * 是否将为空的字段写入到HBase
          * @tparam T
          */
        def hbaseBulkLoadThinRows[T <: HBaseBaseBean[T] : ClassTag](tableName: String,
                                                                    stagingDir: String, insertEmpty: Boolean = true): Unit = {
          this.hbaseContext.bulkLoadThinRows(tableName, rdd, stagingDir, insertEmpty)
        }*/
  }

  /**
    * String类型的RDD扩展
    *
    * @param rdd
    */
  implicit class RDDStringExt(rdd: RDD[String]) {
    // 获取单例的HBaseContext对象
    private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(rdd.sparkContext)

    /**
      * 根据RDD[String]批量删除
      *
      * @param tableName
      * HBase表名
      * @param batchSize
      * 批量删除的大小
      */
    def hbaseBulkDelete(tableName: String, batchSize: Integer = this.hbaseContext.batchSize): Unit = {
      this.hbaseContext.bulkDelete(tableName, rdd, batchSize)
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
      * @tparam E
      * @return
      * 结果集
      */
    def hbaseBulkGet[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], batchSize: Integer = this.hbaseContext.batchSize): RDD[HBaseBaseBean[E]] = {
      this.hbaseContext.bulkGet(tableName, rdd, clazz, batchSize)
    }
  }

  /**
    * RDD相关的扩展
    *
    * @param rdd
    */
  implicit class RDDExt[T: ClassTag](rdd: RDD[T]) {

    def printEachPartition {
      rdd.foreachPartition(it => {
        it.foreach(item => print(item + " "))
      })
    }

    /**
      * 集群模式下打印数据
      */
    def printEachClusterPartition = {
      rdd.collect().foreach(println)
    }

    /**
      * 将rdd转为DataFrame
      */
    def rdd2DataFrame(): DataFrame = {
      lazy val hiveContext = SingletonFactory.getSQLContextInstance(rdd.sparkContext)
      hiveContext.createDataFrame(rdd, classTag[T].runtimeClass)
    }

    /**
      * 将rdd转为DataFrame并注册成临时表
      *
      * @param tableName
      * @return
      */
    def rddRegisterTableAndCache(tableName: String): DataFrame = {
      lazy val hiveContext = SingletonFactory.getSQLContextInstance(rdd.sparkContext)
      val dataFrame = this.rdd2DataFrame()
      dataFrame.registerTempTable(tableName)
      hiveContext.cacheTable(tableName)
      dataFrame
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
      */
    def hbaseInsertRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], batchSize: Int = HBaseSparkBridge.batchSize): Unit = {
      HBaseSparkBridge.hbaseInsertRDD(tableName, rdd, clazz, batchSize)
    }
  }

  /**
    * SparkConf扩展
    *
    * @param sparkConf
    */
  implicit class SparkConfExt(sparkConf: SparkConf) {
    /**
      * 启用并注册kryo序列化
      */
    def kryoRegister(clazz: Class[_]*): SparkConf = {
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialization")
      sparkConf.registerKryoClasses(clazz.toArray)
      sparkConf
    }

    /**
      * 设置Streaming默认配置
      *
      * @return
      */
    def setStreamingDefault(): SparkConf = {
      sparkConf.set("spark.speculation", "true")
        .set("spark.streaming.concurrentJobs", "3")
        .set("spark.default.parallelism", "100")
        .set("spark.speculation.interval", "1000ms")
        .set("spark.speculation.multiplier", "1.8")
        .set("spark.speculation.quantile", "0.1")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
        .set("spark.port.maxRetries", "1000")
      // .setAppName(this.getClass.getSimpleName.replace("$", ""))
      sparkConf
    }

    /**
      * 设置默认配置
      *
      * @return
      */
    def setDefault(): SparkConf = {
      sparkConf.set("spark.broadcast.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec")
        .set("spark.reducer.maxSizeInFlight", "96")
        .set("spark.shuffle.io.maxRetries", "60")
        .set("spark.shuffle.io.retryWait", "60")
        .set("spark.port.maxRetries", "1000")
      sparkConf
    }

    /**
      * 设置名称和配置
      *
      * @return
      */
    def buildConf(): SparkConf = {
      sparkConf.setAppName(GlobalConstants.SparkConf.appName)
      if (GlobalConstants.isLocal) {
        sparkConf.setMaster("local[10]")
      }

      val props = GlobalConstants.SparkConf.sparkConf
      if (StringUtils.isNotBlank(props)) {
        val propArr = props.split("#")
        if (propArr != null && propArr.length > 0) {
          propArr.foreach(prop => {
            if (StringUtils.isNotBlank(prop)) {
              val confArr = prop.split(",")
              if (confArr != null && confArr.length == 2) {
                sparkConf.set(confArr(0), confArr(1))
              }
            }
          })
        }
      }
      sparkConf
    }
  }

  /**
    * SQLContext与HiveContext扩展
    *
    * @param sqlContext
    */
  implicit class SQLContextExt(sqlContext: SQLContext) {

    /**
      * 获取KuduContext实例
      *
      * @return
      */
    def createKuduContext: KuduContextExt = {
      SingletonFactory.getKuduContextInstance(sqlContext.sparkContext)
    }

    /**
      * 判断给定的表是否存在
      *
      * @param tableName
      * 表名
      * @return
      * 存在、不存在
      */
    def tmpTableExists(tableName: String): Boolean = {
      val count = sqlContext.tables().where("tableName='zto_sign_new_kudu' and isTemporary=true").count()
      if (count == 1) true else false
    }

    /**
      * 加载kudu表转为DataFrame
      *
      * @param map
      * @return
      */
    def loadKuduTable(map: Map[String, String]): DataFrame = {
      sqlContext.read.options(map).kudu
    }

    /**
      * 加载kudu表转为DataFrame
      *
      * @param tableName
      * @return
      */
    def loadKuduTable(tableName: String): DataFrame = {
      sqlContext.read.options(Map("kudu.master" -> GlobalConstants.KuduConf.kuduMaster, "kudu.table" -> SparkUtils.packageKuduTableName(tableName))).kudu
    }

    /**
      * 链式设置
      *
      * @return
      */
    def set(key: String, value: String): SQLContext = {
      sqlContext.setConf(key, value)
      sqlContext
    }

    /**
      * 执行一段Hive QL语句，注册为临时表，持久化到hive中
      *
      * @param sqlStr
      * @param tmpTableName
      * @param saveMode
      * 持久化的模式，默认为Overwrite
      * @param cache
      * 默认缓存表
      * @return
      * 生成的DataFrame
      */
    def sqlForPersistent(sqlStr: String, tmpTableName: String, partitionName: String, saveMode: SaveMode = GlobalConstants.SparkConf.saveMode, cache: Boolean = true): DataFrame = {
      val dataFrame = sqlContext.sql(sqlStr)
      val dataFrameWriter = dataFrame.write.mode(saveMode)
      if (StringUtils.isNotBlank(partitionName)) {
        dataFrameWriter.partitionBy(partitionName).saveAsTable(tmpTableName)
      } else {
        dataFrameWriter.saveAsTable(tmpTableName)
      }
      dataFrame
    }

    /**
      * 执行一段Hive QL语句，注册为临时表，并cache
      *
      * @param sqlStr
      * @param tmpTableName
      * @return
      * 生成的DataFrame
      */
    def sqlForCache(sqlStr: String, tmpTableName: String): DataFrame = {
      val dataFrame = sqlContext.sql(sqlStr)
      dataFrame.createOrReplaceTempView(tmpTableName)
      sqlContext.cacheTable(tmpTableName)
      dataFrame
    }

    /**
      * 执行一段Hive QL语句，注册为临时表
      *
      * @param sqlStr
      * @param tmpTableName
      * @return
      * 生成的DataFrame
      */
    def sqlNoCache(sqlStr: String, tmpTableName: String): DataFrame = {
      val dataFrame = sqlContext.sql(sqlStr)
      dataFrame.createOrReplaceTempView(tmpTableName)
      dataFrame
    }

    /**
      * load关系型数据库整张表。若load部分数据，请使用：loadDBToBean()
      *
      * @return
      */
    def loadDBTable(table: String): DataFrame = {
      val props = new Properties()
      props.setProperty("user", GlobalConstants.user)
      props.setProperty("password", GlobalConstants.password)
      props.setProperty("driver", GlobalConstants.driverClass)
      sqlContext.read.jdbc(GlobalConstants.rdburl, table, props)
    }

    /**
      * 从oracle表中load数据
      *
      * @param tableName
      * @param predicates
      * @return
      */
    def loadOracleData(tableName: String, predicates: Array[String]): DataFrame = {
      val props = new Properties()
      props.setProperty("user", GlobalConstants.user)
      props.setProperty("password", GlobalConstants.password)
      props.setProperty("driver", GlobalConstants.driverClass)
      sqlContext.read.jdbc(GlobalConstants.rdburl, tableName, predicates, props)
    }

    /**
      * 批量清空多张缓存表
      *
      * @param tables
      */
    def uncacheTables(tables: String*) = {
      tables.foreach(tableName => {
        if (sqlContext.isCached(tableName)) {
          sqlContext.uncacheTable(tableName)
        }
      })
    }

    /**
      * 批量缓存多张表
      *
      * @param tables
      */
    def cacheTables(tables: String*) = {
      tables.foreach(tableName => {
        sqlContext.cacheTable(tableName)
      })
    }

    /**
      * 删除指定的hive表
      *
      * @param tableNames
      */
    def dropHiveTable(tableNames: String*) = {
      if (ParamUtils.isNotBlank(tableNames)) {
        tableNames.foreach(tableName => {
          sqlContext.sql(s"DROP TABLE IF EXISTS $tableName")
        })
      }
    }

    /**
      * 为指定表添加分区
      *
      * @param tableName
      * 表名
      * @param partitions
      * 分区
      * @return
      */
    def addPartitions(tableName: String, partitions: String*) = {
      if (StringUtils.isNotBlank(tableName) && ParamUtils.isNotBlank(partitions)) {
        partitions.foreach(ds => {
          this.addPartition(tableName, ds, GlobalConstants.SparkConf.partitionName)
        })
      }
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
      * @return
      */
    def addPartition(tableName: String, partition: String, partitionName: String = GlobalConstants.SparkConf.partitionName) = {
      if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(partition) && StringUtils.isNotBlank(partitionName)) {
        sqlContext.sql(s"ALTER TABLE $tableName ADD IF NOT EXISTS partition($partitionName='$partition')")
      }
    }

    /**
      * 为指定表删除分区
      *
      * @param tableName
      * 表名
      * @param partition
      * 分区
      * @return
      */
    def dropPartition(tableName: String, partition: String, partitionName: String = GlobalConstants.SparkConf.partitionName) = {
      if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(partition)) {
        sqlContext.sql(s"ALTER TABLE $tableName DROP IF EXISTS partition($partitionName='$partition')")
      }
    }

    /**
      * 为指定表删除多个分区
      *
      * @param tableName
      * 表名
      * @param partitions
      * 分区
      * @return
      */
    def dropPartitions(tableName: String, partitions: String*) = {
      if (StringUtils.isNotBlank(tableName) && ParamUtils.isNotBlank(partitions)) {
        partitions.foreach(ds => {
          this.dropPartition(tableName, ds, GlobalConstants.SparkConf.partitionName)
        })
      }
    }

    /**
      * 根据给定的表创建新表
      *
      * @param srcTableName
      * @param destTableName
      * @return
      */
    def createTableAsSelect(srcTableName: String, destTableName: String) = {
      if (StringUtils.isNotBlank(srcTableName) && StringUtils.isNotBlank(destTableName)) {
        sqlContext.sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $destTableName AS
             |SELECT * FROM $srcTableName
          """.stripMargin)
      }
    }

    /**
      * 根据一张表创建另一张表
      *
      * @param tableName
      * 表名
      * @param destTableName
      * 目标表名
      */
    def createTableLike(tableName: String, destTableName: String) = {
      if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(destTableName)) {
        sqlContext.sql(
          s"""
             |create table $tableName like $destTableName
          """.stripMargin)
      }
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
      * @return
      */
    def createTableAsSelectFields(srcTableName: String, destTableName: String, cols: String) = {
      if (StringUtils.isNotBlank(srcTableName) && StringUtils.isNotBlank(destTableName) && StringUtils.isNotBlank(cols)) {
        sqlContext.sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $destTableName AS
             |SELECT $cols FROM $srcTableName
          """.stripMargin)
      }
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
      * @return
      */
    def insertIntoPartition(srcTableName: String, destTableName: String, ds: String, cols: String, partitionName: String = GlobalConstants.SparkConf.partitionName) = {
      sqlContext.sql(
        s"""
           |INSERT INTO TABLE $destTableName partition($partitionName='$ds')
           |  SELECT $cols
           |    FROM $srcTableName
        """.stripMargin)
    }

    /**
      * 将sql执行结果插入到目标表指定分区中
      *
      * @param destTableName
      * @param ds
      * @param querySQL
      * @return
      */
    def insertIntoPartitionAsSelect(destTableName: String, ds: String, querySQL: String, partitionName: String = GlobalConstants.SparkConf.partitionName, overwrite: Boolean = false) = {
      val overwriteVal = if (overwrite) "OVERWRITE" else "INTO"
      sqlContext.sql(
        s"""
           |INSERT $overwriteVal TABLE $destTableName partition($partitionName='$ds')
           |  $querySQL
        """.stripMargin)
    }

    /**
      * 将sql执行结果插入到目标表指定分区中
      *
      * @param destTableName
      * @param querySQL
      * @return
      */
    def insertIntoDymPartitionAsSelect(destTableName: String, querySQL: String, partitionName: String = GlobalConstants.SparkConf.partitionName) = {
      sqlContext.sql(
        s"""
           |INSERT INTO TABLE $destTableName partition($partitionName)
           |  $querySQL
        """.stripMargin)
    }

    /**
      * 构建Hive和HBase的映射表
      *
      * @param clazz
      */
    def createHiveHBaseMappingTable[T <: HBaseBaseBean[T]](clazz: Class[T], tableName: String) = {
      if (clazz != null) {
        val obj: T = clazz.newInstance()
        val hql = obj.hive2HBaseMapping(tableName)
        sqlContext.sql(hql)
        sqlContext.createTableAsSelect(s"${tableName}_mapping", tableName)
      }
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
      if (StringUtils.isBlank(oldTableName) || StringUtils.isBlank(newTableName)) {
        return
      }
      val sql = s"ALTER TABLE $oldTableName RENAME TO $newTableName"
      sqlContext.sql(sql)
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
      if (StringUtils.isBlank(tableName) || StringUtils.isBlank(newDB)) {
        return
      }
      val allName = if (StringUtils.isNotBlank(oldDB) && tableName.indexOf(".") == -1) {
        s"$oldDB.$tableName"
      } else {
        tableName
      }
      this.dropHiveTable(s"$newDB.$tableName")
      val sql = s"ALTER TABLE $allName RENAME TO $newDB.$tableName"
      println(sql)
      sqlContext.sql(sql)
    }
  }

  /**
    * DataFrame扩展
    *
    * @param dataFrame
    */
  implicit class DataFrameExt(dataFrame: DataFrame) {

    /**
      * 注册为临时表的同时缓存表
      *
      * @param tmpTableName
      * @return
      * 生成的DataFrame
      */
    def registerTempTableForCache(tmpTableName: String): DataFrame = {
      if (StringUtils.isNotBlank(tmpTableName)) {
        dataFrame.registerTempTable(tmpTableName)
        dataFrame.sqlContext.asInstanceOf[HiveContext].cacheTable(tmpTableName)
      }
      dataFrame
    }

    /**
      * 注册为临时表的同时缓存表，并持久化打Hive中
      *
      * @param tmpTableName
      * 临时表名，与持久化到Hive中的表名一致
      * @param saveMode
      * 默认为Overwrite
      * @param cache
      * 默认cache数据
      * @return
      * 生成的DataFrame
      */
    def registerTempTableForPersistent(tmpTableName: String, saveMode: SaveMode = GlobalConstants.SparkConf.saveMode, cache: Boolean = true): DataFrame = {
      if (StringUtils.isNotBlank(tmpTableName)) {
        dataFrame.write.mode(saveMode).saveAsTable(tmpTableName)
        dataFrame.registerTempTable(tmpTableName)
        if (cache) dataFrame.sqlContext.asInstanceOf[HiveContext].cacheTable(tmpTableName)
      }
      dataFrame
    }

    /**
      * 保存Hive表
      *
      * @param saveMode
      * 保存模式，默认为Overwrite
      * @param partitionName
      * 分区字段
      * @param tableName
      * 表名
      * @return
      * 生成的DataFrame
      */
    def saveAsHiveTable(tableName: String, partitionName: String, saveMode: SaveMode = GlobalConstants.SparkConf.saveMode): DataFrame = {
      if (StringUtils.isNotBlank(tableName)) {
        if (StringUtils.isNotBlank(partitionName)) {
          dataFrame.write.mode(saveMode).partitionBy(partitionName).save(tableName)
        } else {
          dataFrame.write.mode(saveMode).saveAsTable(tableName)
        }
      }
      dataFrame
    }

    /**
      * 将DataFrame数据保存到关系型数据库中
      *
      * @param tableName
      * 关系型数据库表名
      * @return
      */
    def saveAsJDBCTable(tableName: String): Unit = {
      val props = new Properties()
      props.setProperty("user", GlobalConstants.user)
      props.setProperty("password", GlobalConstants.password)
      props.setProperty("driver", GlobalConstants.driverClass)
      dataFrame.write.mode(SaveMode.Append).jdbc(GlobalConstants.rdburl, tableName, props)
    }

    /**
      * 将DataFrame转为List[Bean]，仅限少量数据
      *
      * @param beanClass
      * @return
      */
    def toBeanList[T: ClassTag](beanClass: Class[T]): List[T] = {
      this.dataFrame.map(row => SparkUtils.kuduRowToBean(row, beanClass))(Encoders.bean(beanClass)).collect().toList
    }

    def toBean[T: ClassTag](beanClass: Class[T]): T = {
      this.toBeanList(beanClass).head
    }

    /**
      * 使用Java API的方式将DataFrame中的数据分多个批次插入到HBase中
      *
      * @param tableName
      * HBase表名
      * @param clazz
      * JavaBean类型，为HBaseBaseBean的子类
      * @param batchSize
      * 批次大小
      */
    def hbaseInsertDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], batchSize: Int = HBaseSparkBridge.batchSize): Unit = {
      HBaseSparkBridge.hbaseInsertDF(tableName, this.dataFrame, clazz, batchSize)
    }

    /**
      * 将DataFrame数据打印到控制台
      *
      * @return
      */
    def writeStream2Console: Unit = {
      dataFrame.writeStream.outputMode(OutputMode.Append()).format("console").start().awaitTermination()
    }

    /**
      * 将DataFrame数据写入到carbondata表
      *
      * @return
      */
    def writeStream2Carbon(db: String = GlobalConstants.SparkConf.defaultDB, tableName: String, tigger: Trigger = Trigger.ProcessingTime("5 seconds")): Unit = {
      if (StringUtils.isBlank(db) || StringUtils.isBlank(tableName)) throw new IllegalArgumentException("carbondata的库名或表名不能为空！")
      val carbonTable = CarbonEnv.getCarbonTable(Some(db), tableName)(dataFrame.sparkSession)

      dataFrame.writeStream
        .format("carbondata")
        .trigger(tigger)
        .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath))
        .option("dbName", db)
        .option("tableName", tableName)
        .option(CarbonStreamParser.CARBON_STREAM_PARSER, CarbonStreamParser.CARBON_STREAM_PARSER_ROW_PARSER)
        .start().awaitTermination()
    }

    /**
      * 将DataFrame数据写入到carbondata表中
      * 注：不适用于streaming中调用
      *
      * @param db
      * @param tableName
      * @param partition
      * @param saveMode
      */
    def write2Carbon(db: String = GlobalConstants.SparkConf.defaultDB, tableName: String, partition: String = null, saveMode: SaveMode = SaveMode.Append): Unit = {
      val dfWriter = dataFrame.write.format("carbondata")
        .option("dbName", db)
        .option("tableName", tableName)
      if (StringUtils.isNotBlank(partition)) dfWriter.option("partitionColumns", partition)
      dfWriter.mode(saveMode).save()
    }

    /**
      * 将DataFrame注册为临时表，并缓存表
      *
      * @param tableName
      * 临时表名
      */
    def dataFrameRegisterAndCache(tableName: String): Unit = {
      if (StringUtils.isBlank(tableName)) throw new IllegalArgumentException("临时表名不能为空")
      dataFrame.registerTempTable(tableName)
      dataFrame.sqlContext.cacheTable(tableName)
    }

    /**
      * 将DataFrame数据写入到streaming的carbondata表中
      *
      * @param dbName
      * 数据库名
      * @param tableName
      * 表名
      * @param time
      * rdd时间
      * @param saveMode
      * 追加方式
      */
    def writeStreaming2Carbon(dbName: String = GlobalConstants.SparkConf.defaultDB, tableName: String, time: Time, saveMode: SaveMode = SaveMode.Append): Unit = {
      CarbonSparkStreamingFactory.getStreamSparkStreamingWriter(dataFrame.sparkSession, dbName, tableName)
        .mode(saveMode)
        .writeStreamData(dataFrame, time)
    }

    /**
      * 以merge的方式将数据写入到关系型数据库中
      *
      * @param dbName
      * @param tableName
      */
    def saveToJDBC(dbName: String, tableName: String, saveMode: SaveMode = SaveMode.Append, url: String = GlobalConstants.rdburl, user: String = GlobalConstants.user, password: String = GlobalConstants.password): Unit = {
      if (Objects.isNull(url) || Objects.isNull(user)) throw new IllegalArgumentException("jdbc参数不合法，可将信息放入到配置文件中。")
      dataFrame.write.format("jdbc")
        .option("dbtable", s"$dbName.$tableName")
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .mode(saveMode).save()
    }
  }

  /**
    * StreamingContext扩展
    *
    * @param ssc
    */
  implicit class StreamingContextExt(ssc: StreamingContext) {

    import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
    import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

    /**
      * 创建DStream流
      *
      * @param kafkaParams
      * @param topics
      * @return
      * DStream
      */
    def createDirectStream(kafkaParams: Map[String, Object] = this.kafkaParams(), topics: Set[String] = SparkUtils.topicSplit(GlobalConstants.SparkConf.kafkaTopics), level: StorageLevel = StorageLevel.NONE): DStream[ConsumerRecord[String, String]] = {
      KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    }

    /**
      * kafka配置信息
      *
      * @param groupId
      * 消费组
      * @param offset
      * offset位点，smallest、largest，默认为largest
      * @return
      * kafka相关配置
      */
    def kafkaParams(groupId: String = GlobalConstants.SparkConf.kafkaGroupId, kafkaBrokers: String = GlobalConstants.SparkConf.kafkaBrokers, offset: String = GlobalConstants.SparkConf.kafkaStartingOffset, commit: Boolean = GlobalConstants.SparkConf.kafkaEnableAutoCommit): Map[String, Object] = {
      // 如果配置文件中没有指定spark.kafka.group.id，则默认为appName
      val kafkaGroupId = if (StringUtils.isNotBlank(groupId)) groupId else ssc.sparkContext.appName
      SparkUtils.kafkaParams(kafkaGroupId, kafkaBrokers, offset)
    }

    /**
      * 开启streaming
      */
    def startAwaitTermination(): Unit = {
      ssc.start()
      ssc.awaitTermination()
      ssc.stop(true, true)
    }
  }

  /**
    *
    * @param stream
    */
  implicit class DStreamHBaseExt[T <: HBaseBaseBean[T] : ClassTag](stream: DStream[T]) {
    // 获取单例的HBaseContext对象
    private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(stream.context.sparkContext)

    /**
      * DStrea数据实时写入
      *
      * @param tableName
      * HBase表名
      */
    def streamBulkPut(tableName: String, insertEmpty: Boolean = true): Unit = {
      this.hbaseContext.streamBulkPut(tableName, stream, insertEmpty)
    }
  }

  /**
    * DStream扩展
    *
    * @param stream
    */
  implicit class DStreamExt(stream: DStream[(String, String)]) {

    /**
      * 将kafka过来的json格式数据映射为目标格式DStream
      *
      * @param oggBeanType
      * 对应json消息格式的JavaBean类型
      * @param targetBeanType
      * 目标类型
      * @return
      * 映射后的DStream
      */
    def parseJsonDStream[T <: OGGBaseBean : ClassTag, E <: HBaseBaseBean[E] : ClassTag](oggBeanType: Class[T], targetBeanType: Class[E]): DStream[E] = {
      stream.mapPartitions(it => {
        val oggClazz = classTag[T].runtimeClass
        val targetClazz = classTag[E].runtimeClass
        val getAfterMethod = oggClazz.getMethod("getAfter")
        val buildRowKeyMethod = targetClazz.getMethod("buildRowKey")
        val list = ListBuffer[E]()
        ParserConfig.getGlobalInstance.setAsmEnable(false)
        it.foreach(t => {
          if (StringUtils.isNotBlank(t._2)) {
            try {
              val jsonStr = t._2.trim
              if (jsonStr.startsWith("[") && jsonStr.endsWith("]")) {
                val oggBeanList = JSON.parseArray(jsonStr, oggClazz)
                if (oggBeanList != null && oggBeanList.size() > 0) {
                  val it = oggBeanList.iterator()
                  while (it.hasNext) {
                    val oggBean = it.next()
                    if (oggBean != null) {
                      val after = getAfterMethod.invoke(oggBean)
                      if (after != null) {
                        list += buildRowKeyMethod.invoke(after).asInstanceOf[E]
                      }
                    }
                  }
                }
              } else {
                val oggBean = JSON.parseObject(jsonStr, oggClazz)
                if (oggBean != null) {
                  val after = getAfterMethod.invoke(oggBean)
                  if (after != null) {
                    list += buildRowKeyMethod.invoke(after).asInstanceOf[E]
                  }
                }
              }
            } catch {
              case e: Exception => println(t._2)
            }
          }
        })
        list.iterator
      })
    }
  }

  implicit class JavaCollectionExt[T](collection: java.util.Collection[T]) {
    def toScalaList: mutable.Buffer[T] = {
      JavaConversions.asScalaBuffer(collection.asInstanceOf[java.util.List[T]])
    }

    def toScalaSet: mutable.Set[T] = {
      JavaConversions.asScalaSet(collection.asInstanceOf[java.util.Set[T]])
    }
  }

  implicit class JavaMapExt[K, V](map: java.util.Map[K, V]) {
    def toScalaMap: mutable.Map[K, V] = {
      JavaConversions.mapAsScalaMap(map)
    }
  }

}