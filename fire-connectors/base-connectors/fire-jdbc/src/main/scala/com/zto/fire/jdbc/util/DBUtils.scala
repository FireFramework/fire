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

package com.zto.fire.jdbc.util

import com.google.common.collect.Maps
import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.conf.{FireFrameworkConf, KeyNum}
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.{Logging, ReflectionUtils}
import com.zto.fire.jdbc.JdbcConf
import com.zto.fire.jdbc.conf.FireJdbcConf
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

import java.sql.{Date, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.XADataSource
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * 关系型数据库操作工具类
 *
 * @author ChengLong 2019-6-23 11:16:18
 */
object DBUtils extends Logging {
  private lazy val driverFile = "driver.properties"
  // 读取配置文件，获取jdbc url与driver的映射关系
  private lazy val driverMap = {
    tryWithReturn {
      val properties = new Properties()
      properties.load(this.getClass.getClassLoader.getResourceAsStream(this.driverFile))
      Maps.fromProperties(properties)
    } (this.logger, s"加载${this.driverFile}成功", s"加载${this.driverFile}失败，请确认该配置文件是否存在！")
  }

  /**
   * 将ResultSet结果转为JavaBean集合
   *
   * @param rs    数据库中的查询结果集
   * @param clazz 目标JavaBean类型
   * @return 将ResultSet转换为JavaBean集合返回
   */
  def resultSet2BeanList[T](rs: ResultSet, clazz: Class[T]): ListBuffer[T] = {
    val list = ListBuffer[T]()
    val fields = clazz.getDeclaredFields
    try {
      val columnMap = this.columns(rs)
      while (rs.next()) {
        val obj = clazz.newInstance()
        fields.foreach(field => {
          ReflectionUtils.setAccessible(field)
          val anno = field.getAnnotation(classOf[FieldName])
          if (!(anno != null && anno.disuse())) {
            val fieldName = if (anno != null && StringUtils.isNotBlank(anno.value())) anno.value() else field.getName
            if (columnMap.containsKey(fieldName)) {
              val fieldType = field.getType
              if (fieldType eq classOf[JString]) field.set(obj, rs.getString(fieldName))
              else if (fieldType eq classOf[JInt]) field.set(obj, rs.getInt(fieldName))
              else if (fieldType eq classOf[JDouble]) field.set(obj, rs.getDouble(fieldName))
              else if (fieldType eq classOf[JLong]) field.set(obj, rs.getLong(fieldName))
              else if (fieldType eq classOf[JBigDecimal]) field.set(obj, rs.getBigDecimal(fieldName))
              else if (fieldType eq classOf[JFloat]) field.set(obj, rs.getFloat(fieldName))
              else if (fieldType eq classOf[JBoolean]) field.set(obj, rs.getBoolean(fieldName))
              else if (fieldType eq classOf[JShort]) field.set(obj, rs.getShort(fieldName))
              else if (fieldType eq classOf[java.sql.Date]) field.set(obj, rs.getDate(fieldName))
              else if (fieldType eq classOf[java.sql.Time]) field.set(obj, rs.getTime(fieldName))
              else if (fieldType eq classOf[java.sql.Timestamp]) field.set(obj, rs.getTimestamp(fieldName))
              else if (fieldType eq classOf[JByte]) field.set(obj, rs.getByte(fieldName))
              else if (fieldType eq classOf[java.sql.Array]) field.set(obj, rs.getArray(fieldName))
              else if (fieldType eq classOf[java.sql.Blob]) field.set(obj, rs.getBlob(fieldName))
              else if (fieldType eq classOf[java.sql.Clob]) field.set(obj, rs.getClob(fieldName))
              else throw new IllegalArgumentException(s"JDBC查询存在不兼容的数据类型，JavaBean中字段名称：${fieldName} 字段类型：${fieldType}")
            }
          }
        })
        list += obj
      }
    } catch {
      case e: Exception =>
        logError("ResultSet转换成JavaBean过程中出现异常.", e)
        throw e
    }
    list
  }

  /**
   * 判断指定的结果集中是否包含指定的列名
   *
   * @param rs
   * 关系型数据库查询结果集
   * @param columnName
   * 列名
   * @return
   * true: 存在 false：不存在
   */
  def containsColumn(rs: ResultSet, columnName: String): Boolean = {
    val start = currentTime
    val retVal = Try {
      try {
        rs.findColumn(columnName)
      }
    }
    if (retVal.isFailure) logWarning(s"ResultSet结果集中未找到列名：${columnName}，请保证ResultSet与JavaBean中的字段一一对应，耗时：${elapsed(start)}")
    retVal.isSuccess
  }

  /**
   * 根据查询结果集获取字段名称与类型的映射关系
   * @param rs
   * jdbc query结果集
   * @return
   * Map[FieldName, FieldType]
   */
  def columns(rs: ResultSet): JHashMap[String, Int] = {
    val metaData = rs.getMetaData
    val fieldMap = new JHashMap[String, Int]()
    for (i <- 1 to metaData.getColumnCount) {
      val fieldName = if (FireJdbcConf.useLabel) {
        // 支持别名映射
        metaData.getColumnLabel(i)
      } else {
        metaData.getColumnName(i)
      }
      val fieldType = metaData.getColumnType(i)
      fieldMap.put(fieldName, fieldType)
    }
    fieldMap
  }

  /**
   * 获取ResultSet返回的记录数
   *
   * @param rs
   * 查询结果集
   * @return
   * 结果集行数
   */
  def rowCount(rs: ResultSet): Int = {
    if (rs == null) return 0
    rs.last()
    val count = rs.getRow
    rs.beforeFirst()
    count
  }

  /**
   * 获取jdbc连接信息，若调用者指定，以调用者为准，否则读取配置文件
   *
   * @param jdbcProps
   * 调用者传入的jdbc配置信息
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * jdbc配置信息
   */
  def getJdbcProps(jdbcProps: Properties = null, keyNum: Int = KeyNum._1): Properties = {
    if (jdbcProps == null || jdbcProps.size() == 0) {
      val defaultProps = new Properties()
      defaultProps.setProperty("user", FireJdbcConf.user(keyNum))
      defaultProps.setProperty("password", FireJdbcConf.password(keyNum))
      defaultProps.setProperty("driver", FireJdbcConf.driverClass(keyNum))
      defaultProps.setProperty("batchsize", FireJdbcConf.batchSize(keyNum).toString)
      defaultProps.setProperty("isolationLevel", FireJdbcConf.isolationLevel(keyNum).toUpperCase)
      defaultProps
    } else {
      jdbcProps
    }
  }

  /**
   * 根据jdbc驱动包名或数据库url区分连接的不同的数据库厂商标识
   */
  def dbTypeParser(driverClass: String, url: String): String = {
    var dbType = "unknown"
    Datasource.values().map(_.toString).foreach(datasource => {
      if (driverClass.toUpperCase.contains(datasource)) dbType = datasource
    })

    // 尝试从url中的端口号解析，对结果进行校正，因为有些数据库使用的是mysql驱动，可以通过url中的端口号区分
    if (StringUtils.isNotBlank(url)) {
      FireFrameworkConf.lineageDatasourceMap.foreach(kv => {
        if (url.contains(kv._2)) dbType = kv._1.toUpperCase
      })
    }
    dbType
  }

  /**
   * 通过解析jdbc url，返回url对应的已知的driver class
   *
   * @param url
   * jdbc url
   * @return
   * driver class
   */
  def parseDriverByUrl(url: String): String = {
    var driver = ""
    // 尝试从url中的端口号解析，对结果进行校正，因为有些数据库使用的是mysql驱动，可以通过url中的端口号区分
    if (StringUtils.isNotBlank(url)) {
      this.driverMap.foreach(kv => {
        if (url.toLowerCase.contains(kv._1)) driver = kv._2
      })
    }
    driver
  }

  /**
   * 根据指定的列集合，反射将JavaBean中的成员变量设置到Jdbc的PreparedStatement中
   *
   * @param columns
   * 列的集合
   * @param statement
   * JDBC的PreparedStatement对象
   * @param bean
   * 从指定的JavaBean中获取数据填充到PreparedStatement中
   */
  def setPreparedStatement[T](columns: Seq[String], statement: PreparedStatement, bean: T): Unit = {
    requireNonEmpty(columns, bean)("字段列表或JavaBean不能为空！")

    val fields = ReflectionUtils.getAllFields(bean.getClass)
    for (i <- columns.indices) {
      val field = fields.get(columns(i))
      requireNonNull(field)(s"未在${bean.getClass}中找到字段${columns(i)}，请检查SQL语句或JavaBean的定义")

      field.getType.getName.replace("java.lang.", "") match {
        case "Integer" | "int" => statement.setInt(i + 1, field.get(bean).asInstanceOf[JInt])
        case "Long" | "long" => statement.setLong(i + 1, field.get(bean).asInstanceOf[JLong])
        case "Boolean" | "boolean" => statement.setBoolean(i + 1, field.get(bean).asInstanceOf[JBoolean])
        case "Float" | "float" => statement.setFloat(i + 1, field.get(bean).asInstanceOf[JFloat])
        case "Double" | "double" => statement.setDouble(i + 1, field.get(bean).asInstanceOf[JDouble])
        case "java.math.BigDecimal" => statement.setBigDecimal(i + 1, field.get(bean).asInstanceOf[JBigDecimal])
        case "java.sql.Date" => statement.setDate(i + 1, field.get(bean).asInstanceOf[Date])
        case _ => try {
          statement.setString(i + 1, field.get(bean).toString)
        } catch {
          case e: Throwable => logError(s"字段类型不匹配，请检查${columns(i)}与JavaBean的对应关系", e)
        }
      }
    }
  }

  /**
   * 根据传入的驱动类构建XADataSource实例
   *
   * @param jdbcConf
   * 数据源链接信息
   * @param dbType
   * 对应数据源的类型
   * MySQL、Oracle、PostgreSQL
   * @return
   * 对应数据源的XADataSource实例
   */
  def buildXADataSource(jdbcConf: JdbcConf, dbType: Datasource): XADataSource = {
    val driverClass = dbType match {
      case Datasource.MYSQL => "com.mysql.jdbc.jdbc2.optional.MysqlXADataSource"
      case Datasource.ORACLE => "oracle.jdbc.xa.OracleXADataSource"
      case Datasource.PostgreSQL => "oracle.jdbc.xa.OracleXADataSource"
      case _ => ""
    }

    requireNonEmpty(driverClass)("当前只支持MySQL、Oracle与PostgreSQL等数据库！")
    tryWithReturn {
      val mysqlXADataSourceClass = Class.forName(driverClass)
      val mysqlXADataSource = mysqlXADataSourceClass.newInstance()

      val setUrl = ReflectionUtils.getMethodByName(mysqlXADataSourceClass, "setUrl")
      setUrl.invoke(mysqlXADataSource, jdbcConf.url)

      val setUser = ReflectionUtils.getMethodByName(mysqlXADataSourceClass, "setUser")
      setUser.invoke(mysqlXADataSource, jdbcConf.username)

      val setPassword = ReflectionUtils.getMethodByName(mysqlXADataSourceClass, "setPassword")
      setPassword.invoke(mysqlXADataSource, jdbcConf.password)

      mysqlXADataSource.asInstanceOf[XADataSource]
    }(this.logger, catchLog = "获取MySQL XADataSource失败，请检查是否引入MySQL相关JDBC驱动依赖", hook = true)
  }
}
