package com.zto.fire.core.util

import java.lang.reflect.Field
import java.sql.ResultSet
import java.text.NumberFormat
import java.util.{Locale, Properties}

import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.util._
import com.zto.fire.core.ext.module.KuduContextExt
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.rocketmq.spark.RocketMQConfig
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect._
import scala.util.Try


/**
 * Spark 相关的工具类
 * Created by ChengLong on 2016-11-24.
 */
object SparkUtils {

  /**
   * 将Row转为自定义bean，以JavaBean中的Field为基准
   * bean中的field名称要与DataFrame中的field名称保持一致
   *
   * @param row
   * @return
   */
  def sparkRowToBean[T](row: Row, clazz: Class[T]): T = {
    val obj = clazz.newInstance()
    if (row != null && clazz != null) {
      try {
        clazz.getDeclaredFields.foreach(field => {
          field.setAccessible(true)
          val anno = field.getAnnotation(classOf[FieldName])
          val begin = if (anno == null) true else !anno.disuse()
          if (begin) {
            val fieldName = if (anno != null && StringUtils.isNotBlank(anno.value())) anno.value() else field.getName
            val index = row.fieldIndex(fieldName.trim)
            val fieldType = field.getType
            if (fieldType eq classOf[String]) field.set(obj, row.getString(index))
            else if (fieldType eq classOf[java.lang.Integer]) field.set(obj, row.getAs[IntegerType](index))
            else if (fieldType eq classOf[java.lang.Double]) field.set(obj, row.getAs[DoubleType](index))
            else if (fieldType eq classOf[java.lang.Long]) field.set(obj, row.getAs[LongType](index))
            else if (fieldType eq classOf[java.math.BigDecimal]) field.set(obj, row.getAs[DecimalType](index))
            else if (fieldType eq classOf[java.lang.Float]) field.set(obj, row.getAs[FloatType](index))
            else if (fieldType eq classOf[java.lang.Boolean]) field.set(obj, row.getAs[BooleanType](index))
            else if (fieldType eq classOf[java.lang.Short]) field.set(obj, row.getAs[ShortType](index))
            else if (fieldType eq classOf[java.util.Date]) field.set(obj, row.getAs[DateType](index))
          }
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    obj
  }

  /**
   * 将SparkRow迭代映射为对象的迭代
   *
   * @param it
   * Row迭代器
   * @param clazz
   * 待映射的自定义JavaBean
   * @tparam T
   * 泛型
   * @return
   * 映射为对象的集合
   */
  def sparkRowToBean[T](it: Iterator[Row], clazz: Class[T], toUppercase: Boolean = false): Iterator[T] = {
    val list = ListBuffer[T]()
    if (it != null && clazz != null) {
      val fields = clazz.getDeclaredFields
      it.foreach(row => {
        val obj = clazz.newInstance()
        fields.foreach(field => {
          field.setAccessible(true)
          val anno = field.getAnnotation(classOf[FieldName])
          val begin = if (anno == null) true else !anno.disuse()
          if (begin) {
            var fieldName = if (anno != null && StringUtils.isNotBlank(anno.value())) anno.value() else field.getName
            fieldName = if (toUppercase) fieldName.toUpperCase else fieldName
            if (this.containsColumn(row, fieldName)) {
              val index = row.fieldIndex(fieldName.trim)
              val fieldType = field.getType
              if (fieldType eq classOf[String]) field.set(obj, row.getString(index))
              else if (fieldType eq classOf[java.lang.Integer]) field.set(obj, row.getAs[IntegerType](index))
              else if (fieldType eq classOf[java.lang.Long]) field.set(obj, row.getAs[LongType](index))
              else if (fieldType eq classOf[java.math.BigDecimal]) field.set(obj, row.getAs[DecimalType](index))
              else if (fieldType eq classOf[java.lang.Boolean]) field.set(obj, row.getAs[BooleanType](index))
              else if (fieldType eq classOf[java.lang.Double]) field.set(obj, row.getAs[DoubleType](index))
              else if (fieldType eq classOf[java.lang.Float]) field.set(obj, row.getAs[FloatType](index))
              else if (fieldType eq classOf[java.lang.Short]) field.set(obj, row.getAs[ShortType](index))
              else if (fieldType eq classOf[java.util.Date]) field.set(obj, row.getAs[DateType](index))
            }
          }
        })
        list += obj
      })
    }
    list.iterator
  }

  /**
   * 判断指定的Row中是否包含指定的列名
   *
   * @param row
   * DataFrame中的行
   * @param columnName
   * 列名
   * @return
   * true: 存在 false：不存在
   */
  def containsColumn(row: Row, columnName: String): Boolean = {
    Try {
      try {
        row.fieldIndex(columnName)
      }
    }.isSuccess
  }

  /**
   * 根据实体bean构建schema信息
   *
   * @return StructField集合
   */
  def buildSchemaFromBean(beanClazz: Class[_], upper: Boolean = false): List[StructField] = {
    val fieldMap = ReflectionUtils.getAllFields(beanClazz)
    val strutFields = new ListBuffer[StructField]()
    import scala.collection.JavaConversions._
    for (map <- fieldMap.entrySet) {
      val field: Field = map.getValue
      val fieldType: Class[_] = field.getType
      val anno: FieldName = field.getAnnotation(classOf[FieldName])
      var fieldName: String = map.getKey
      var nullable: Boolean = true
      val disuse = if (anno == null) {
        false
      } else {
        if (StringUtils.isNotBlank(anno.value)) {
          fieldName = anno.value
        }
        nullable = anno.nullable()
        anno.disuse()
      }
      if (!disuse) {
        if (upper) fieldName = fieldName.toUpperCase
        if (fieldType eq classOf[String]) strutFields += DataTypes.createStructField(fieldName, DataTypes.StringType, nullable)
        else if (fieldType eq classOf[java.lang.Integer]) strutFields += DataTypes.createStructField(fieldName, DataTypes.IntegerType, nullable)
        else if (fieldType eq classOf[java.lang.Double]) strutFields += DataTypes.createStructField(fieldName, DataTypes.DoubleType, nullable)
        else if (fieldType eq classOf[java.lang.Long]) strutFields += DataTypes.createStructField(fieldName, DataTypes.LongType, nullable)
        else if (fieldType eq classOf[java.math.BigDecimal]) strutFields += DataTypes.createStructField(fieldName, DataTypes.DoubleType, nullable)
        else if (fieldType eq classOf[java.lang.Float]) strutFields += DataTypes.createStructField(fieldName, DataTypes.FloatType, nullable)
        else if (fieldType eq classOf[java.lang.Boolean]) strutFields += DataTypes.createStructField(fieldName, DataTypes.BooleanType, nullable)
        else if (fieldType eq classOf[java.lang.Short]) strutFields += DataTypes.createStructField(fieldName, DataTypes.ShortType, nullable)
        else if (fieldType eq classOf[java.util.Date]) strutFields += DataTypes.createStructField(fieldName, DataTypes.DateType, nullable)
      }
    }
    strutFields.toList
  }

  /**
   * 获取kafka中json数据的before和after信息
   *
   * @param beanClazz
   * json数据对应的java bean类型
   * @param isMySQL
   * 是否为mysql解析的消息
   * @param fieldNameUpper
   * 字段名称是否为大写
   * @param parseAll
   * 是否解析所有字段信息
   * @return
   */
  def buildSchema2Kafka(beanClazz: Class[_], parseAll: Boolean = false, isMySQL: Boolean = true, fieldNameUpper: Boolean = false): StructType = {
    if (parseAll) {
      val structTypes = new StructType()
        .add("table", StringType)
        .add("op_type", StringType)
        .add("op_ts", StringType)
        .add("current_ts", StringType)
        .add("gtid", StringType)
        .add("logFile", StringType)
        .add("offset", StringType)
        .add("schema", StringType)
        .add("when", StringType)
        .add("after", StructType(SparkUtils.buildSchemaFromBean(beanClazz, fieldNameUpper)))
        .add("before", StructType(SparkUtils.buildSchemaFromBean(beanClazz, fieldNameUpper)))
      if (isMySQL) structTypes.add("pos", LongType) else structTypes.add("pos", StringType)
    } else {
      new StructType().add("table", StringType)
        .add("after", StructType(SparkUtils.buildSchemaFromBean(beanClazz, fieldNameUpper)))
    }
  }


  /**
   * 以Map的方式获取Hive表的字段名称和类型
   *
   * @param tableName
   *                  db.hiveTable
   * @return
   * Map[FieldName, FieldType]
   */
  def getTableSchemaAsMap(hiveContext: HiveContext, kuduContext: KuduContextExt, tableName: String): Map[String, String] = {
    val dataFrame = if (tableName.startsWith("impala")) {
      kuduContext.loadKuduTable(tableName)
    } else {
      hiveContext.table(tableName)
    }

    dataFrame.schema.map(s => {
      (s.name, s.dataType.simpleString)
    }).toMap
  }

  /**
   * 获取表的全名
   *
   * @param dbName
   * 表所在的库名
   * @param tableName
   * 表名
   * @return
   * 库名.表名
   */
  def getFullTableName(dbName: String = GlobalConstants.SparkConf.defaultDB, tableName: String): String = {
    val dbNameStr = if (StringUtils.isBlank(dbName)) GlobalConstants.SparkConf.defaultDB else dbName
    s"$dbNameStr.$tableName"
  }

  /**
   * 分割topic列表，返回set集合
   *
   * @param topics
   * 多个topic以指定分隔符分割
   * @return
   */
  def topicSplit(topics: String, splitStr: String = ","): Set[String] = {
    ValueUtils.requireNonNullForce(topics, "topic不能为空，请在配置文件中[ spark.kafka.topics ]配置")
    topics.split(splitStr).filter(topic => StringUtils.isNotBlank(topic)).map(topic => topic.trim).toSet
  }

  /**
   * 获取webui地址
   *
   * @param spark
   * @return
   */
  def getWebUI(spark: SparkSession): String = {
    val optConf = spark.conf.getOption("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")

    if (optConf.isDefined && StringUtils.isNotBlank(optConf.get)) {
      optConf.get.replace("\\", "")
        .replace(GlobalConstants.Strings.hostNamePrefix, GlobalConstants.Strings.ipPrefxi)
    } else {
      spark.sparkContext.uiWebUrl.get.replace(GlobalConstants.Strings.hostNamePrefix, GlobalConstants.Strings.ipPrefxi)
    }
  }

  /**
   * 获取applicationId
   *
   * @param spark
   * @return
   */
  def getApplicationId(spark: SparkSession): String = {
    spark.sparkContext.applicationId
  }

  /**
   * kafka配置信息
   *
   * @param groupId
   * 消费组
   * @param offset
   * smallest、largest
   * @return
   * kafka相关配置
   */
  def kafkaParams(groupId: String = null, kafkaBrokers: String = null, offset: String = null, autoCommit: Boolean = false, keyNum: Int = 1): Map[String, Object] = {
    ValueUtils.requireNonNull(groupId, s"kafka groupId不能为空，请在配置文件中指定：spark.kafka.group.id$keyNum 指定")

    val finalKafkaBrokers = if (StringUtils.isBlank(kafkaBrokers)) GlobalConstants.KafkaConf.kafkaBrokers(keyNum) else kafkaBrokers
    val finalOffset = if (StringUtils.isBlank(offset)) GlobalConstants.KafkaConf.kafkaStartingOffset(keyNum) else offset
    val finalAutoCommit = if (autoCommit == null) GlobalConstants.KafkaConf.kafkaEnableAutoCommit(keyNum) else autoCommit

    val consumerMap = collection.mutable.Map[String, Object](
      "bootstrap.servers" -> finalKafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> finalOffset,
      "enable.auto.commit" -> (finalAutoCommit: java.lang.Boolean),
      "session.timeout.ms" -> GlobalConstants.KafkaConf.kafkaSessionTimeOut(keyNum),
      "request.timeout.ms" -> GlobalConstants.KafkaConf.kafkaRequestTimeOut(keyNum),
      "max.poll.interval.ms" -> GlobalConstants.KafkaConf.kafkaPollInterval(keyNum)
    )

    // 心跳间隔时间
    val heartbeatInterval = GlobalConstants.KafkaConf.kafkaHeartbeatInterval(keyNum)
    if (heartbeatInterval > 0) {
      consumerMap += ("heartbeat.interval.ms" -> heartbeatInterval)
    }
    // 消费者组最大的session超时时间
    val groupMaxSessionTimeOut = GlobalConstants.KafkaConf.kafkaGroupMaxSessionTimeOut(keyNum)
    if (groupMaxSessionTimeOut > 0) {
      consumerMap += ("group.max.session.timeout.ms" -> groupMaxSessionTimeOut)
    }
    // 消费者组最小的session超时时间
    val groupMinSessionTimeOut = GlobalConstants.KafkaConf.kafkaGroupMinSessionTimeOut(keyNum)
    if (groupMinSessionTimeOut > 0) {
      consumerMap += ("group.min.session.timeout.ms" -> groupMinSessionTimeOut)
    }
    // 一次调用pool返回的最大记录数
    val maxPollRecords = GlobalConstants.KafkaConf.kafkaMaxPollRecords(keyNum)
    if (maxPollRecords > 0) {
      consumerMap += ("max.poll.records" -> maxPollRecords)
    }
    // 每个分区返回的最大数据量
    val maxPartitionFetchBytes = GlobalConstants.KafkaConf.kafkaMaxPartitionFetchBytes(keyNum)
    if (maxPartitionFetchBytes > 0) {
      consumerMap += ("max.partition.fetch.bytes" -> maxPartitionFetchBytes)
    }

    consumerMap.toMap
  }

  /**
   * rocketMQ配置信息
   *
   * @param groupId
   * 消费组
   * @return
   * rocketMQ相关配置
   */
  def rocketParams(groupId: String = null, rocketNameServer: String = null, tag: String = null, keyNum: Int = 1): java.util.Map[String, String] = {
    ValueUtils.requireNonNull(groupId, s"RocketMQ的groupId不能为空，请在配置文件中指定：spark.rocket.group.id$keyNum")
    val finalNameServer = if (StringUtils.isBlank(rocketNameServer)) GlobalConstants.RocketConf.rocketNameServer(keyNum) else rocketNameServer
    val finalTag = if (StringUtils.isBlank(tag)) GlobalConstants.RocketConf.rocketConsumerTag(keyNum) else tag

    val optionParams = new java.util.HashMap[String, String]()
    optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, finalNameServer)
    optionParams.put(RocketMQConfig.MAX_PULL_SPEED_PER_PARTITION, "5000")
    optionParams.put(RocketMQConfig.CONSUMER_GROUP, groupId)
    optionParams.put(RocketMQConfig.CONSUMER_TAG, finalTag)

    val nameserverPollInterval = GlobalConstants.RocketConf.rocketNameserverPollInterval(keyNum)
    if (StringUtils.isNotBlank(nameserverPollInterval)) optionParams.put(RocketMQConfig.NAME_SERVER_POLL_INTERVAL, nameserverPollInterval)
    val brokerserverHeartbeatInterval = GlobalConstants.RocketConf.rocketBrokerserverHeartbeatInterval(keyNum)
    if (StringUtils.isNotBlank(brokerserverHeartbeatInterval)) optionParams.put(RocketMQConfig.BROKER_HEART_BEAT_INTERVAL, brokerserverHeartbeatInterval)
    val consumerOffsetPersistInterval = GlobalConstants.RocketConf.rocketConsumerOffsetPersistInterval(keyNum)
    if (StringUtils.isNotBlank(consumerOffsetPersistInterval)) optionParams.put(RocketMQConfig.CONSUMER_OFFSET_PERSIST_INTERVAL, consumerOffsetPersistInterval)
    val consumerMaxThreads = GlobalConstants.RocketConf.rocketConsumerMaxThreads(keyNum)
    if (StringUtils.isNotBlank(consumerMaxThreads)) optionParams.put(RocketMQConfig.CONSUMER_MAX_THREADS, consumerMaxThreads)
    val consumerMinThreads = GlobalConstants.RocketConf.rocketConsumerMinThreads(keyNum)
    if (StringUtils.isNotBlank(consumerMinThreads)) optionParams.put(RocketMQConfig.CONSUMER_MIN_THREADS, consumerMinThreads)
    val spoutMessagesMaxRetry = GlobalConstants.RocketConf.rocketSpoutMessagesMaxRetry(keyNum)
    if (StringUtils.isNotBlank(spoutMessagesMaxRetry)) optionParams.put(RocketMQConfig.MESSAGES_MAX_RETRY, spoutMessagesMaxRetry)
    val pullMaxSpeedPerPartition = GlobalConstants.RocketConf.rocketPullMaxSpeedPerPartition(keyNum)
    if (StringUtils.isNotBlank(pullMaxSpeedPerPartition)) optionParams.put(RocketMQConfig.MAX_PULL_SPEED_PER_PARTITION, pullMaxSpeedPerPartition)
    val pullMaxBatchSize = GlobalConstants.RocketConf.rocketPullMaxBatchSize(keyNum)
    if (StringUtils.isNotBlank(pullMaxBatchSize)) optionParams.put(RocketMQConfig.PULL_MAX_BATCH_SIZE, pullMaxBatchSize)
    val pullTimeoutMs = GlobalConstants.RocketConf.rocketPullTimeoutMs(keyNum)
    if (StringUtils.isNotBlank(pullTimeoutMs)) optionParams.put(RocketMQConfig.PULL_TIMEOUT_MS, pullTimeoutMs)

    optionParams
  }

  /**
   * 使用配置文件中的spark.streaming.batch.duration覆盖传参的batchDuration
   *
   * @param batchDuration
   *                   代码中指定的批次时间
   * @param hotRestart 是否热重启，热重启优先级最高
   * @return
   * 被配置文件覆盖后的批次时间
   */
  def overrideBatchDuration(batchDuration: Long, hotRestart: Boolean): Long = {
    if (hotRestart) return batchDuration
    val confBathDuration = PropUtils.getInt(GlobalConstants.PropKeys.SPARK_STREAMING_BATCH_DURATION, -1)
    if (confBathDuration == -1) {
      batchDuration
    } else {
      Math.abs(confBathDuration)
    }
  }

  /**
   * 获取spark任务的webUI地址信息
   *
   * @return
   */
  def getUI(webUI: String): String = {
    val line = new StringBuilder()
    webUI.split(",").foreach(url => {
      line.append(StringsUtils.hrefTag(url) + StringsUtils.brTag(""))
    })

    line.toString()
  }

  /**
   * 用于判断当前是否为executor
   *
   * @return true: executor false: driver
   */
  def isExecutor: Boolean = {
    val executorId = this.getExecutorId
    if ("driver".equalsIgnoreCase(executorId)) {
      false
    } else {
      true
    }
  }

  /**
   * 获取当前executor id
   *
   * @return
   * executor id或driver
   */
  def getExecutorId: String = {
    SparkEnv.get.executorId
  }

  /**
   * 用于判断当前是否为driver
   *
   * @return true: driver false: executor
   */
  def isDriver: Boolean = {
    !this.isExecutor
  }

  /**
   * 是否是集群模式
   *
   * @return
   * true: 集群模式  false：本地模式
   */
  def isCluster: Boolean = {
    SystemInfoUtils.isLinux
  }

  /**
   * 是否是本地模式
   *
   * @return
   * true: 本地模式  false：集群模式
   */
  def isLocal: Boolean = {
    !isCluster
  }

  /**
   * 判断是否为yarn-client模式
   *
   * @return
   * true: yarn-client模式
   */
  def isYarnClientMode: Boolean = {
    "client".equalsIgnoreCase(this.deployMode)
  }

  /**
   * 判断是否为yarn-cluster模式
   *
   * @return
   * true: yarn-cluster模式
   */
  def isYarnClusterMode: Boolean = {
    "cluster".equalsIgnoreCase(this.deployMode)
  }

  /**
   * 获取spark任务运行模式
   */
  def deployMode: String = {
    SingletonFactory.getSparkSession.conf.get("spark.submit.deployMode")
  }

  /**
   * 优先从配置文件中获取配置信息，若获取不到，则从SparkEnv中获取
   *
   * @param key
   * 配置的key
   * @param default
   * 配置为空则返回default
   * @return
   * 配置的value
   */
  def getConf(key: String, default: String = ""): String = {
    var value = PropUtils.getString(key, default)
    if (StringUtils.isBlank(value) && SparkEnv.get != null) {
      value = SparkEnv.get.conf.get(key, default)
    }
    value
  }

  /**
   * 将指定的schema转为小写
   *
   * @param schema
   * 转为小写的列
   * @return
   * 转为小写的field数组
   */
  def schemaToLowerCase(schema: StructType): ArrayBuffer[String] = {
    val cols = ArrayBuffer[String]()
    schema.foreach(field => {
      val fieldName = field.name
      cols += (s"$fieldName as ${fieldName.toLowerCase}")
    })
    cols
  }

  /**
   * 将内部row类型的DataFrame转为Row类型的DataFrame
   *
   * @param df
   * InternalRow类型的DataFrame
   * @return
   * Row类型的DataFrame
   */
  def toExternalRow(df: DataFrame): DataFrame = {
    val schema = df.schema
    val mapedRowRDD = df.queryExecution.toRdd.mapPartitions { rows =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      rows.map(converter(_).asInstanceOf[Row])
    }
    SingletonFactory.getSparkSession.createDataFrame(mapedRowRDD, schema)
  }
}
