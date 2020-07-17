package com.zto.fire.core.util

import java.lang.reflect.Field

import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.conf.{FireHiveConf, FireSparkConf, FireStringConf}
import com.zto.fire.common.util._
import com.zto.fire.core.ext.module.KuduContextExt
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try


/**
 * Spark 相关的工具类
 * Created by ChengLong on 2016-11-24.
 */
object SparkUtils {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

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
            if (this.containsColumn(row, fieldName.trim)) {
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
  def getFullTableName(dbName: String = FireSparkConf.defaultDB, tableName: String): String = {
    val dbNameStr = if (StringUtils.isBlank(dbName)) FireSparkConf.defaultDB else dbName
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
        .replace(FireStringConf.hostNamePrefix, FireStringConf.ipPrefxi)
    } else {
      spark.sparkContext.uiWebUrl.get.replace(FireStringConf.hostNamePrefix, FireStringConf.ipPrefxi)
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
    val confBathDuration = FireSparkConf.confBathDuration
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
    if (SparkEnv.get != null) SparkEnv.get.executorId else ""
  }

  /**
   * 获取入口类名
   */
  def getMainClass: String = {
    if (SparkEnv.get != null) SparkEnv.get.conf.get("spark.driver.class.name", "") else ""
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

  /**
   * 从配置文件中读取并执行hive set的sql
   */
  def executeHiveConfSQL(spark: SparkSession): Unit = {
    if (spark != null) {
      val confMap = FireHiveConf.hiveConfMap
      LogUtils.logMap(this.logger, confMap, "Execute hive sql conf.")
    }
  }
}
