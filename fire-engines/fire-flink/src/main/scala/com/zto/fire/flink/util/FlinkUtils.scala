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

package com.zto.fire.flink.util

import com.google.common.collect.HashBasedTable
import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.util._
import com.zto.fire.flink.bean.FlinkTableSchema
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.sql.FlinkSqlExtensionsParser
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.predef._
import com.zto.fire.{JHashMap, JStringBuilder, noEmpty}
import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.{SqlParser => CalciteParser}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.ExecutionConfig.ClosureCleanerLevel
import org.apache.flink.api.common.{ExecutionConfig, ExecutionMode, InputDependencyConstraint}
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import org.apache.flink.table.api.{SqlDialect => FlinkSqlDialect}
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.data.{DecimalData, GenericRowData, RowData}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row

import java.net.{URL, URLClassLoader}
import scala.util.Try

/**
 * flink相关工具类
 *
 * @author ChengLong 2020年1月16日 16:28:23
 * @since 0.4.1
 */
object FlinkUtils extends Serializable with Logging {
  // 维护schema、fieldName与fieldIndex关系
  private[this] val schemaTable = HashBasedTable.create[FlinkTableSchema, String, Int]
  private var jobManager: Option[Boolean] = None
  private var mode: Option[String] = None
  lazy val calciteParserConfig = this.createParserConfig
  lazy val calciteHiveParserConfig = this.createHiveParserConfig

  /**
   * 构建flink default的SqlParser config
   */
  def createParserConfig(dialect: FlinkSqlDialect = FlinkSqlDialect.DEFAULT): CalciteParser.Config = {
    val configBuilder = CalciteParser.configBuilder
      .setQuoting(Quoting.BACK_TICK)
      .setUnquotedCasing(Casing.TO_UPPER)
      .setQuotedCasing(Casing.UNCHANGED)

    if (dialect == FlinkSqlDialect.DEFAULT) configBuilder.setParserFactory(FlinkSqlParserImpl.FACTORY) else configBuilder.setParserFactory(FlinkHiveSqlParserImpl.FACTORY)
    configBuilder.build
  }

  /**
   * 构建flink default的SqlParser config
   */
  private[this] def createParserConfig: CalciteParser.Config = this.createParserConfig()

  /**
   * 构建flink hive方言版的SqlParser config
   */
  private[this] def createHiveParserConfig: CalciteParser.Config = this.createParserConfig(FlinkSqlDialect.HIVE)

  /**
   * 根据sql构建Calcite SqlParser
   */
  def sqlNodeParser(sql: String, config: CalciteParser.Config = this.createParserConfig): SqlNode = {
    CalciteParser.create(sql, config).parseStmt()
  }

  /**
   * SQL血缘解析
   */
  def sqlParser(sql: String): Unit = FlinkSqlExtensionsParser.sqlParse(sql)

  /**
   * SQL语法校验，如果语法错误，则返回错误堆栈
   * @param sql
   * sql statement
   */
  def sqlValidate(sql: String): Try[Unit] = {
    val retVal = Try {
      try {
        // 使用默认的sql解析器解析
        val sqlNode = this.sqlNodeParser(sql)
      } catch {
        case e: Throwable => {
          // 使用hive方言语法解析器解析
          val sqlNode = this.sqlNodeParser(sql, this.calciteHiveParserConfig)
        }
      }
    }

    if (retVal.isFailure) {
      ExceptionBus.post(retVal.failed.get, sql)
    }

    retVal
  }

  /**
   * SQL语法校验
   * @param sql
   * sql statement
   * @return
   * true：校验成功 false：校验失败
   */
  def sqlLegal(sql: String): Boolean = this.sqlValidate(sql).isSuccess

  /**
   * 将schema、fieldName与fieldIndex信息维护到table中
   */
  private[this] def extendSchemaTable(schema: FlinkTableSchema): Unit = {
    if (schema != null && !schemaTable.containsRow(schema)) {
      for (i <- 0 until schema.getFieldCount) {
        schemaTable.put(schema, schema.getFieldName(i).get(), i)
      }
    }
  }

  /**
   * 将Row转为自定义bean，以JavaBean中的Field为基准
   * bean中的field名称要与DataFrame中的field名称保持一致
   *
   * @return
   */
  def rowToBean[T](schema: FlinkTableSchema, row: Row, clazz: Class[T]): T = {
    requireNonEmpty(schema, row, clazz)
    val obj = clazz.newInstance()
    tryWithLog {
      this.extendSchemaTable(schema)
      clazz.getDeclaredFields.foreach(field => {
        ReflectionUtils.setAccessible(field)
        val anno = field.getAnnotation(classOf[FieldName])
        val begin = if (anno == null) true else !anno.disuse()
        if (begin) {
          val fieldName = if (anno != null && ValueUtils.noEmpty(anno.value())) anno.value().trim else field.getName
          if (this.schemaTable.contains(schema, fieldName)) {
            val fieldIndex = this.schemaTable.get(schema, fieldName)
            field.set(obj, row.getField(fieldIndex))
          }
        }
      })
      if (obj.isInstanceOf[HBaseBaseBean[T]]) {
        val method = ReflectionUtils.getMethodByName(clazz, "buildRowKey")
        if (method != null) method.invoke(obj)
      }
    }(this.logger, catchLog = "flink row转为JavaBean过程中发生异常.")
    obj
  }

  /**
   * 解析并设置配置文件中的配置信息
   */
  def parseConf(config: ExecutionConfig): ExecutionConfig = {
    requireNonEmpty(config)("Flink配置实例不能为空")

    // flink.auto.generate.uid.enable=true 默认为：true
    if (FireFlinkConf.autoGenerateUidEnable) {
      config.enableAutoGeneratedUIDs()
    } else {
      config.disableAutoGeneratedUIDs()
    }

    // flink.auto.type.registration.enable=true 默认为：true
    if (!FireFlinkConf.autoTypeRegistrationEnable) {
      config.disableAutoTypeRegistration()
    }

    // flink.force.avro.enable=true  默认值为：false
    if (FireFlinkConf.forceAvroEnable) {
      config.enableForceAvro()
    } else {
      config.disableForceAvro()
    }

    // flink.force.kryo.enable=true 默认值为：false
    if (FireFlinkConf.forceKryoEnable) {
      config.enableForceKryo()
    } else {
      config.disableForceKryo()
    }

    // flink.generic.types.enable=true  默认值为：true
    if (FireFlinkConf.genericTypesEnable) {
      config.enableGenericTypes()
    } else {
      config.disableGenericTypes()
    }

    // flink.object.reuse.enable=true 默认值为：false
    if (FireFlinkConf.objectReuseEnable) {
      config.enableObjectReuse()
    } else {
      config.disableObjectReuse()
    }

    // flink.auto.watermark.interval=0  默认值为：0
    if (FireFlinkConf.autoWatermarkInterval != -1) config.setAutoWatermarkInterval(FireFlinkConf.autoWatermarkInterval)

    // flink.closure.cleaner.level=recursive  默认值为：RECURSIVE，包括：RECURSIVE、NONE、TOP_LEVEL
    if (StringUtils.isNotBlank(FireFlinkConf.closureCleanerLevel)) config.setClosureCleanerLevel(ClosureCleanerLevel.valueOf(FireFlinkConf.closureCleanerLevel.toUpperCase))

    // flink.default.input.dependency.constraint=any  默认值：ANY，包括：ANY、ALL
    if (StringUtils.isNotBlank(FireFlinkConf.defaultInputDependencyConstraint)) config.setDefaultInputDependencyConstraint(InputDependencyConstraint.valueOf(FireFlinkConf.defaultInputDependencyConstraint.toUpperCase))

    // flink.execution.mode=pipelined 默认值：PIPELINED，包括：PIPELINED、PIPELINED_FORCED、BATCH、BATCH_FORCED
    if (StringUtils.isNotBlank(FireFlinkConf.executionMode)) config.setExecutionMode(ExecutionMode.valueOf(FireFlinkConf.executionMode.toUpperCase))

    // flink.latency.tracking.interval=0  默认值：0
    if (FireFlinkConf.latencyTrackingInterval != -1) config.setLatencyTrackingInterval(FireFlinkConf.latencyTrackingInterval)

    // flink.max.parallelism=1  没有默认值
    if (FireFlinkConf.maxParallelism != -1) config.setMaxParallelism(FireFlinkConf.maxParallelism)

    // flink.task.cancellation.interval=1 无默认值
    if (FireFlinkConf.taskCancellationInterval != -1) config.setTaskCancellationInterval(FireFlinkConf.taskCancellationInterval)

    // flink.task.cancellation.timeout.millis=1000  无默认值
    if (FireFlinkConf.taskCancellationTimeoutMillis != -1) config.setTaskCancellationTimeout(FireFlinkConf.taskCancellationTimeoutMillis)

    // flink.use.snapshot.compression=false 默认值：false
    config.setUseSnapshotCompression(FireFlinkConf.useSnapshotCompression)

    config
  }

  /**
   * 加载指定路径下的udf jar包
   */
  def loadUdfJar: Unit = {
    val udfJarUrl = PropUtils.getString(FireFlinkConf.FLINK_SQL_CONF_UDF_JARS, "")
    if (StringUtils.isBlank(udfJarUrl)) {
      logWarning(s"flink udf jar包路径不能为空，请在配置文件中通过：${FireFlinkConf.FLINK_SQL_CONF_UDF_JARS}=/path/to/udf.jar 指定")
      return
    }

    val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    val classLoader = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader]
    method.invoke(classLoader, new URL(udfJarUrl))
  }

  /**
   * 判断当前环境是否为JobManager
   */
  def isJobManager: Boolean = {
    if (this.jobManager.isEmpty) {
      val envClass = Class.forName("org.apache.flink.runtime.util.EnvironmentInformation")
      if (ReflectionUtils.containsMethod(envClass, "isJobManager")) {
        val method = envClass.getMethod("isJobManager")
        jobManager = Some((method.invoke(null) + "").toBoolean)
      } else {
        logError("未找到方法：EnvironmentInformation.isJobManager()")
      }
    }
    jobManager.getOrElse(true)
  }

  /**
   * 用于判断当前是否为driver
   *
   * @return true: driver false: executor
   */
  def isMaster: Boolean = this.isJobManager

  /**
   * 判断当前环境是否为TaskManager
   */
  def isTaskManager: Boolean = !this.isJobManager

  /**
   * 获取flink的运行模式
   */
  def deployMode: String = {
    if (this.mode.isEmpty) {
      val globalConfClass = Class.forName("org.apache.flink.configuration.GlobalConfiguration")
      if (ReflectionUtils.containsMethod(globalConfClass, "getRunMode")) {
        val method = globalConfClass.getMethod("getRunMode")
        this.mode = Some(method.invoke(null) + "")
      } else {
        logError("未找到方法：GlobalConfiguration.getRunMode()")
      }
    }
    val deployMode = this.mode.getOrElse("yarn-per-job")
    if (isEmpty(deployMode) || "null".equalsIgnoreCase(deployMode)) "local" else deployMode
  }

  /**
   * 判断当前运行模式是否为yarn-application模式
   */
  def isYarnApplicationMode: Boolean = "yarn-application".equalsIgnoreCase(this.deployMode)

  /**
   * 判断当前运行模式是否为yarn-per-job模式
   */
  def isYarnPerJobMode: Boolean = "yarn-per-job".equalsIgnoreCase(this.deployMode)

  /**
   * 将Javabean中匹配的field值转为RowData
   *
   * @param bean
   * 任意符合JavaBean规范的实体对象
   * @return
   * RowData实例
   */
  def bean2RowData(bean: Object, rowType: RowType): RowData = {
    requireNonEmpty(bean, rowType)

    val genericRowData = new GenericRowData(rowType.getFieldCount)
    val fieldNames = rowType.getFieldNames
    val clazz = bean.getClass

    // 以建表语句中声明的字段列表为标准进行循环
    for (pos <- 0 until rowType.getFieldCount) {
      // 根据临时表的字段名称获取JavaBean中对应的同名的field的值
      val field = ReflectionUtils.getFieldByName(clazz, fieldNames.get(pos))
      requireNonEmpty(field, s"JavaBean中未找到名为${fieldNames.get(pos)}的field，请检查sql建表语句或JavaBean的声明！")

      val value = field.get(bean).toString
      // 进行类型匹配，将获取到的JavaBean中的字段值映射为SQL建表语句中所指定的类型，并设置到对应的field中
      rowType.getTypeAt(pos).toString match {
        case "INT" | "TINYINT" | "SMALLINT" | "INTEGER" => genericRowData.setField(pos, value.toInt)
        case "BIGINT" => genericRowData.setField(pos, value.toLong)
        case "DOUBLE" => genericRowData.setField(pos, value.toDouble)
        case "FLOAT" => genericRowData.setField(pos, value.toFloat)
        case "BOOLEAN" => genericRowData.setField(pos, value.toBoolean)
        case "BYTE" => genericRowData.setField(pos, value.toByte)
        case "SHORT" => genericRowData.setField(pos, value.toShort)
        case fieldType if fieldType.contains("DECIMAL") => {
          // 获取SQL建表语句中的DECIMAL字段的精度
          val accuracy = rowType.getTypeAt(pos).toString.replace("DECIMAL(", "").replace(")", "").split(",")
          genericRowData.setField(pos, DecimalData.fromBigDecimal(new JBigDecimal(value), accuracy(0).trim.toInt, accuracy(1).trim.toInt))
        }
        case _ => genericRowData.setField(pos, new BinaryStringData(value))
      }
    }
    genericRowData
  }

  /**
   * 将RowData中匹配的field值转为Javabean
   *
   * @param clazz
   * 任意符合JavaBean规范的Class类型
   * @return
   * JavaBean实例
   */
  def rowData2Bean[T](clazz: Class[T], rowType: RowType, rowData: RowData): T = {
    requireNonEmpty(clazz, rowData)
    val bean = clazz.newInstance()

    val fieldNames = rowType.getFieldNames

    // 以建表语句中声明的字段列表为标准进行循环
    for (pos <- 0 until rowType.getFieldCount) {
      // 根据临时表的字段名称获取JavaBean中对应的同名的field的值
      val field = ReflectionUtils.getFieldByName(clazz, fieldNames.get(pos))
      requireNonEmpty(field, s"JavaBean中未找到名为${fieldNames.get(pos)}的field，请检查sql建表语句或JavaBean的声明！")

      // 进行类型匹配，将获取到的JavaBean中的字段值映射为SQL建表语句中所指定的类型，并设置到对应的field中
      rowType.getTypeAt(pos).toString match {
        case "INT" | "TINYINT" | "SMALLINT" | "INTEGER" => field.setInt(bean, rowData.getInt(pos))
        case "BIGINT" => field.setLong(bean, rowData.getLong(pos))
        case "DOUBLE" => field.setDouble(bean, rowData.getDouble(pos))
        case "FLOAT" => field.setFloat(bean, rowData.getFloat(pos))
        case "BOOLEAN" => field.setBoolean(bean, rowData.getBoolean(pos))
        case "BYTE" => field.setByte(bean, rowData.getByte(pos))
        case "SHORT" => field.setShort(bean, rowData.getShort(pos))
        case fieldType if fieldType.contains("DECIMAL") => {
          // 获取SQL建表语句中的DECIMAL字段的精度
          val accuracy = rowType.getTypeAt(pos).toString.replace("DECIMAL(", "").replace(")", "").split(",")
          field.set(bean, rowData.getDecimal(pos, accuracy(0).trim.toInt, accuracy(1).trim.toInt))
        }
        case _ => field.set(bean, rowData.getString(pos).toString)
      }
    }
    bean
  }

  /**
   * 获取JobManager或TaskManager的标识
   * @return
   * JobManager/container_xxx_xxx_xx_xxxx
   */
  def getResourceId: String = {
    if (isJobManager) "JobManager" else PropUtils.getString("taskmanager.resource-id", OSUtils.getHostName)
  }

  /**
   * 获取applicationId
   */
  def getApplicationId: String = PropUtils.getString("high-availability.cluster-id")

  /**
   * 获取flink版本号
   */
  def getVersion: String = EnvironmentInformation.getVersion

  /**
   * 替换sql中with表达式的value部分
   * 如果配置中有与sql中相同的信息，则会被替换
   *
   * @param originalSql
   * 原始含有敏感信息的SQL语句
   * @return
   * 替换敏感信息后的SQL语句
   */
  def sqlWithConfReplace(originalSql: String): String = {
    if (!FireFlinkConf.sqlWithReplaceModeEnable) return originalSql
    var replacedSql = originalSql
    val repMap = new JHashMap[String, String]()

    // 正则匹配with表达式中的value部分
    RegularUtils.withValueReg.findAllMatchIn(replacedSql).foreach(matchStr => {
      val withValue = matchStr.matched
      if (noEmpty(withValue)) {
        val matchValue = RegularUtils.valueReg.findFirstIn(withValue)
        if (matchValue.isDefined) {
          val oldValue = matchValue.get
          // 判断sql中的值与配置信息是否有匹配，存在匹配项则放入到map中等待下一步批量替换
          val confValue = PropUtils.getString(matchValue.get.replace("'", ""), "")
          if (noEmpty(confValue)) {
            val replacedValue = if (noEmpty(confValue)) confValue else oldValue
            repMap.put(oldValue, s"'${replacedValue}'")
          }
        }
      }
    })

    // 将存在配置的值进行替换
    repMap.foreach(kv => {
      replacedSql = replacedSql.replace(kv._1, kv._2)
    })

    replacedSql
  }

  /**
   * 替换sql中with表达式的options
   * 包含value变量替换与datasource数据源整体替换
   *
   * @param originalSql
   * 原始含有敏感信息的SQL语句
   * @return
   * 替换敏感信息后的SQL语句
   */
  def sqlWithReplace(originalSql: String): String = {
    val replacedSql = this.replaceSqlAlias(this.sqlWithConfReplace(originalSql))
    logDebug("Flink Sql with options替换成功，最终SQL：" + replacedSql)
    replacedSql
  }

  /**
   * 替换Flink Sql with表达式中的options选项，规则如下：
   *
   * 获取所有flink.sql.with.为前缀的配置信息如：
   * flink.sql.with.bill_db.connector	=	mysql
   * flink.sql.with.bill_db.url			  =	jdbc:mysql://localhost:3306/fire
   * 上述配置标识定义名为bill_db的数据源，配置了两个options选项分别为：
   * connector	=	mysql
   * url			  =	jdbc:mysql://localhost:3306/fire
   * sql中即可通过 'datasource'='bill_db' 引用到上述两项option
   */
  def replaceSqlAlias(sql: String): String = {
    if (!FireFlinkConf.sqlWithReplaceModeEnable) return sql

    var replacedSql = sql
    val matchDatasource = RegularUtils.withDatasourceReg.findFirstIn(sql)
    if (matchDatasource.isDefined) {
      val matchValue = RegularUtils.withValueReg.findFirstIn(matchDatasource.get)
      if (matchValue.isDefined) {
        // 获取 'datasource'='value' 中的value值
        val datasource = matchValue.get.replaceAll("=", "").replace("'", "").trim
        if (noEmpty(datasource)) {
          val optionsText = new JStringBuilder
          FireFlinkConf.flinkSqlWithOptions.foreach(options => {
            if (options._1.startsWith(s"${datasource}.")) {
              // 将配置文件中定义的数据源options拼接成flink sql with字句中的options：'key' = 'value'
              optionsText.append(s"""\t'${options._1.replace(s"${datasource}.", "")}'='${options._2}',\n""")
            }
          })

          val optionsList = optionsText.toString
          if (noEmpty(optionsList)) {
            // 移除动态拼接的option列表中最后一行的逗号
            val replaceLast = optionsList.substring(0, optionsList.lastIndexOf(","))
            replacedSql = RegularUtils.withDatasourceReg.replaceFirstIn(sql, replaceLast)
          }
        }
      }
    }

    replacedSql
  }

  /**
   * 用于判断引擎是否已完成上下文的初始化
   * 1. Spark：SparkContext
   * 2. Flink: ExecutionEnv
   */
  def isEngineUp: Boolean = FlinkSingletonFactory.streamEnv != null

  /**
   * 用于判断引擎是否已销毁上下文
   * 1. Spark：SparkContext
   * 2. Flink: ExecutionEnv
   */
  def isEngineDown: Boolean = {
    // TODO: 判断上下文退出
    FlinkSingletonFactory.streamEnv == null || (FlinkSingletonFactory.streamEnv != null)
  }
}
