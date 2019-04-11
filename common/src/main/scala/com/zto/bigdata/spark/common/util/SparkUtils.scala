package com.zto.bigdata.spark.common.util

import java.lang.reflect.Field
import java.sql.ResultSet
import java.text.NumberFormat
import java.util.{Date, Locale}

import com.zto.bigdata.spark.common.anno.FieldName
import com.zto.bigdata.spark.common.ext.KuduContextExt
import com.zto.bigdata.spark.common.ext.SparkExt._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import spark.{Request, Response}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect._


/**
  * Spark 相关的工具类
  * Created by ChengLong on 2016-11-24.
  */
object SparkUtils {

  /**
    * 将scan对象转为String
    *
    * @param scan
    * @return
    */
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  /**
    * 将给定的字符串补齐指定的位数
    *
    * @param str
    * @param length
    * @return
    */
  def appendString(str: String, char: String, length: Int): String = {
    if (StringUtils.isNotBlank(str) && StringUtils.isNotBlank(char) && length > str.length) {
      val sb: StringBuilder = new StringBuilder(str)
      var i: Int = 0
      while (i < length - str.length) {
        sb.append(char)
        i += 1
      }
      sb.toString
    } else if (length == str.length) {
      str
    } else if (length < str.length && length > 0) {
      str.substring(0, length)
    } else {
      ""
    }
  }

  /**
    * 将kudu的JavaBean转为Row
    * 实体Class类型
    *
    * @return
    * Spark SQL Row对象
    */
  def kuduBean2Row[T: ClassTag](bean: T): Row = {
    val beanClazz = classTag[T].runtimeClass
    val values = ListBuffer[AnyRef]()
    beanClazz.getDeclaredFields.foreach(field => {
      field.setAccessible(true)
      val anno = field.getAnnotation(classOf[FieldName])
      if (anno != null && anno.id()) {
        values += field.get(bean)
      }
    })
    Row(values: _*)
  }

  /**
    * 将kudu的JavaBean转为Row
    *
    * @param beanClazz
    * 实体Class类型
    * @return
    * Spark SQL Row对象
    */
  def bean2Row(beanClazz: Class[_]): Row = {
    val fieldList = ListBuffer[Field]()
    beanClazz.getDeclaredFields.foreach(field => {
      field.setAccessible(true)
      val anno = field.getAnnotation(classOf[FieldName])
      val begin = if (anno == null) true else !anno.disuse()
      if (begin) {
        fieldList += field
      }
    })
    Row(fieldList)
  }

  /**
    * 将row结果转为javabean
    *
    * @param row 数据库中的一条记录
    * @param clazz
    * @tparam T
    * @return
    */
  def dbRow2Bean[T](row: ResultSet, clazz: Class[T]): T = {
    val obj = clazz.newInstance()
    clazz.getDeclaredFields.foreach(field => {
      field.setAccessible(true)
      val fieldType = field.getType
      val anno = field.getAnnotation(classOf[FieldName])
      val fieldName = if (anno != null) anno.value() else field.getName

      if (fieldType eq classOf[String]) field.set(obj, row.getString(fieldName))
      else if (fieldType eq classOf[java.lang.Integer]) field.set(obj, row.getInt(fieldName))
      else if (fieldType eq classOf[java.lang.Double]) field.set(obj, row.getDouble(fieldName))
      else if (fieldType eq classOf[java.lang.Long]) field.set(obj, row.getLong(fieldName))
      else if (fieldType eq classOf[java.math.BigDecimal]) field.set(obj, row.getBigDecimal(fieldName))
      else if (fieldType eq classOf[java.lang.Float]) field.set(obj, row.getFloat(fieldName))
      else if (fieldType eq classOf[java.lang.Boolean]) field.set(obj, row.getBoolean(fieldName))
      else if (fieldType eq classOf[java.lang.Short]) field.set(obj, row.getShort(fieldName))
      else if (fieldType eq classOf[java.util.Date]) field.set(obj, row.getDate(fieldName))
    })
    obj
  }

  /**
    * 将ResultSet结果转为javabean
    *
    * @param rs 数据库中的查询结果集
    * @param clazz
    * @tparam T
    * @return
    */
  def dbResultSet2Bean[T](rs: ResultSet, clazz: Class[T]): ListBuffer[T] = {
    val list = ListBuffer[T]()
    val fields = clazz.getDeclaredFields
    try {
      while (rs.next()) {
        var obj = clazz.newInstance()
        fields.foreach(field => {
          field.setAccessible(true)
          val fieldType = field.getType
          val anno = field.getAnnotation(classOf[FieldName])
          if (!(anno != null && anno.disuse())) {
            val fieldName = if (anno != null && StringUtils.isNotBlank(anno.value())) anno.value() else field.getName
            if (fieldType eq classOf[String]) field.set(obj, rs.getString(fieldName))
            else if (fieldType eq classOf[java.lang.Integer]) field.set(obj, rs.getInt(fieldName))
            else if (fieldType eq classOf[java.lang.Double]) field.set(obj, rs.getDouble(fieldName))
            else if (fieldType eq classOf[java.lang.Long]) field.set(obj, rs.getLong(fieldName))
            else if (fieldType eq classOf[java.math.BigDecimal]) field.set(obj, rs.getBigDecimal(fieldName))
            else if (fieldType eq classOf[java.lang.Float]) field.set(obj, rs.getFloat(fieldName))
            else if (fieldType eq classOf[java.lang.Boolean]) field.set(obj, rs.getBoolean(fieldName))
            else if (fieldType eq classOf[java.lang.Short]) field.set(obj, rs.getShort(fieldName))
            else if (fieldType eq classOf[Date]) field.set(obj, rs.getDate(fieldName))
          }
        })
        list += obj
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    list
  }

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
            val fieldName = if (anno != null) anno.value() else field.getName
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
  def sparkRowToBean[T](it: Iterator[Row], clazz: Class[T]): Iterator[T] = {
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
            val fieldName = if (anno != null) anno.value() else field.getName
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
        })
        list += obj
      })
    }
    list.iterator
  }

  /**
    * 将Row转为自定义bean，以Row中的Field为基准
    * bean中的field名称要与DataFrame中的field名称保持一致
    *
    * @param row
    * @return
    */
  def kuduRowToBean[T](row: Row, clazz: Class[T]): T = {
    val obj = clazz.newInstance()
    if (row != null && clazz != null) {
      try {
        row.schema.fieldNames.foreach(fieldName => {
          clazz.getDeclaredFields.foreach(field => {
            field.setAccessible(true)
            if (field.getName.equalsIgnoreCase(fieldName)) {
              val index = row.fieldIndex(fieldName)
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
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    obj
  }

  /**
    * 获取系统当前时间，精确到秒
    *
    * @return
    */
  def currentTime = {
    System.currentTimeMillis() / 1000
  }

  /**
    * 计算运行时长
    *
    * @param startTime
    */
  def runTime(startTime: Long) = {
    val currentTime = this.currentTime
    val apartTime = currentTime - startTime
    val hours = apartTime / 3600
    val hoursStr = if (hours < 10) s"0${hours}" else s"${hours}"
    val minutes = apartTime / 60 - hours * 60
    val minutesStr = if (minutes < 10) s"0${minutes}" else s"${minutes}"
    val seconds = apartTime - minutes * 60 - hours * 60 * 60
    val secondsStr = if (seconds < 10) s"0${seconds}" else s"${seconds}"

    s"${hoursStr}时 ${minutesStr}分 ${secondsStr}秒"
  }

  /**
    * 数据格式转换
    *
    * @return
    */
  def numberFormat(num: Long): String = {
    val numberFormat = NumberFormat.getInstance(Locale.CHINA);
    numberFormat.format(num)
  }

  /**
    * 根据实体bean构建schema信息
    *
    * @return StructField集合
    */
  def buildSchemaFromBean(beanClazz: Class[_]): List[StructField] = {
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
    * @param requireBefore
    * 是否解析before信息
    * @return
    */
  def buildSchema2Kafka(beanClazz: Class[_], requireBefore: Boolean = false): StructType = {
    val schema = new StructType().add("table", StringType)
      .add("after", StructType(SparkUtils.buildSchemaFromBean(beanClazz)))
    if (requireBefore) schema.add("before", StructType(SparkUtils.buildSchemaFromBean(beanClazz)))
    schema
  }

  /**
    * 根据实体bean构建kudu表schema（只构建主键字段）
    *
    * @return StructField集合
    */
  def buildSchemaFromKuduBean(beanClazz: Class[_]): List[StructField] = {
    val fieldMap = ReflectionUtils.getAllFields(beanClazz)
    val strutFields = new ListBuffer[StructField]()
    import scala.collection.JavaConversions._
    for (map <- fieldMap.entrySet) {
      val field: Field = map.getValue
      val fieldType: Class[_] = field.getType
      val anno: FieldName = field.getAnnotation(classOf[FieldName])
      var fieldName: String = map.getKey
      var nullable: Boolean = true
      val begin = if (anno == null) {
        false
      } else {
        if (StringUtils.isNotBlank(anno.value)) {
          fieldName = anno.value
        }
        nullable = anno.nullable()
        !anno.disuse
      }
      if (begin && anno.id) {
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
    * 将表名包装为以impala::开头的表
    *
    * @param tableName
    * 库名.表名
    * @return
    * 包装后的表名
    */
  def packageKuduTableName(tableName: String): String = {
    if (StringUtils.isBlank(tableName)) throw new IllegalArgumentException("表名不能为空")
    if (tableName.startsWith("impala::")) {
      tableName
    } else {
      s"impala::$tableName"
    }
  }

  /**
    * 将Bean转为bytes
    *
    * @param obj
    * @param updateNull
    * 为空的也将被覆盖
    * @tparam T
    * @return
    */
  def bean2Bytes[T: ClassTag](obj: T, updateNull: Boolean = true): (Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])]) = {
    if (obj == null || obj.getClass == null) throw new IllegalArgumentException("对象不能为空")
    val clazz = obj.getClass
    clazz.getMethod("buildRowKey").invoke(obj)
    var rowKey = ""
    val arrays = ArrayBuffer[(Array[Byte], Array[Byte], Array[Byte])]()

    ReflectionUtils.getAllFields(clazz).toScalaMap.foreach(t => {
      val key = t._1
      val field = t._2
      val objValue = field.get(obj)
      if (StringUtils.isBlank(rowKey) && "rowKey".equals(key)) {
        rowKey = field.get(obj).asInstanceOf[String]
      }
      val fieldName = clazz.getAnnotation(classOf[FieldName])
      val famliyByte = if (fieldName != null && StringUtils.isNotBlank(fieldName.family())) fieldName.family().getBytes else GlobalConstants.familyName.getBytes
      val goOn = if (fieldName == null) true else fieldName != null && !fieldName.disuse()
      if (goOn) {
        val keyByte = key.getBytes
        if (objValue != null) {
          val objValueStr = objValue.toString
          val fieldType = field.getType
          if (fieldType eq classOf[String]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr)))
          else if (fieldType eq classOf[Integer]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toInt)))
          else if (fieldType eq classOf[Double]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toDouble)))
          else if (fieldType eq classOf[Long]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toLong)))
          else if (fieldType eq classOf[BigDecimal]) arrays += ((famliyByte, keyByte, Bytes.toBytes(new java.math.BigDecimal(objValueStr))))
          else if (fieldType eq classOf[Float]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toFloat)))
          else if (fieldType eq classOf[Boolean]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toBoolean)))
          else if (fieldType eq classOf[Short]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toShort)))
        } else if (updateNull) {
          arrays += ((famliyByte, keyByte, null))
        }
      }
    })
    (rowKey.getBytes, arrays.toArray)
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
    if (StringUtils.isBlank(topics)) {
      throw new IllegalArgumentException("topic不合法")
    } else {
      topics.split(splitStr).toSet
    }
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
}
