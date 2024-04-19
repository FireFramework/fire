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

package com.zto.fire.common.lineage

import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.lineage.{Lineage, SQLTable, SQLTablePartitions}
import com.zto.fire.common.conf.FireFrameworkConf._
import com.zto.fire.common.conf.{FireHiveConf, FireKafkaConf, FirePS1Conf, FireRocketMQConf}
import com.zto.fire.common.enu.{Datasource, Operation, ThreadPoolType}
import com.zto.fire.common.lineage.LineageManager.printLog
import com.zto.fire.common.lineage.parser.ConnectorParserManager
import com.zto.fire.common.lineage.parser.connector._
import com.zto.fire.common.util._
import com.zto.fire.predef._

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions

/**
 * 用于统计当前任务使用到的数据源信息，包括MQ、DB、hive等连接信息等
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-11-26 15:30
 */
private[fire] class LineageManager extends Logging {
  // 用于存放当前任务用到的数据源信息
  private[fire] lazy val lineageMap = new ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]()
  // 用于收集来自不同数据源的sql语句，后续会异步进行SQL解析，考虑到分布式场景下会有很多重复的SQL执行，因此使用了线程不安全的队列即可满足需求
  private lazy val dbSqlQueue = new ConcurrentLinkedQueue[DBSqlSource]()
  // 用于解析数据源的异步定时调度线程
  private lazy val parserExecutor = ThreadUtils.createThreadPool("LineageManager", ThreadPoolType.SCHEDULED).asInstanceOf[ScheduledExecutorService]
  private lazy val parseCount = new AtomicInteger()
  private lazy val addDBCount = new AtomicInteger()
  // 用于收集各实时引擎执行的sql语句
  this.lineageParse()

  /**
   * 用于异步解析sql中使用到的表，并放到linageMap中
   */
  private[this] def lineageParse(): Unit = {
    if (lineageEnable) {
      this.parserExecutor.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = {
          if (!lineageEnable || (parseCount.incrementAndGet() >= lineageRunCount && !parserExecutor.isShutdown)) {
            disableLineage()
            parserExecutor.shutdown()
            printLog("实时血缘解析任务退出！")
          }

          // 1. 解析jdbc sql语句
          parseJdbcSql()
          printLog(s"完成第${parseCount}/${lineageRunCount}次解析JDBC中的血缘信息")

          // 2. 将SQL中使用到的的表血缘信息映射到数据源中
          LineageManager.mapTableToDatasource(SQLLineageManager.getSQLLineage.getTables)
          printLog(s"完成第${parseCount}/${lineageRunCount}次异步解析SQL埋点中的表信息")
        }
      }, lineageRunInitialDelay, lineageRunPeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 解析来自于jdbc的sql血缘
   */
  private[this] def parseJdbcSql(): Unit = {
    tryWithLog {
      for (_ <- 0 until dbSqlQueue.size()) {
        val sqlSource = dbSqlQueue.poll()
        if (sqlSource != null) {
          printLog(s"解析JDBC SQL：${sqlSource.sql}")
          val jdbcLineages = SQLUtils.parseLineage(sqlSource.sql)
          jdbcLineages.foreach(lineage => {
            val operations = new JHashSet[Operation]()
            operations.add(lineage._2)
            add(Datasource.parse(sqlSource.datasource), DBDatasource(sqlSource.datasource, sqlSource.cluster, lineage._1, sqlSource.username, operation = operations))
          })
        }
      }
    }(logger, "", "jdbc血缘信息解析失败", isThrow = false, hook = false)
  }

  /**
   * 添加一个数据源描述信息
   */
  private[fire] def add(sourceType: Datasource, datasourceDesc: DatasourceDesc): Unit = this.synchronized {
    if (!lineageEnable || this.lineageMap.size() > lineageMaxSize) return

    printLog(s"1. 合并数据源add之前，lineageMap：$lineageMap 目标datasource：$datasourceDesc")
    val set = this.lineageMap.mergeGet(sourceType)(new JHashSet[DatasourceDesc]())
    if (set.isEmpty) set.add(datasourceDesc)
    val mergedSet = this.mergeDatasource(set, datasourceDesc)
    this.lineageMap.put(sourceType, mergedSet)
    printLog(s"2. 合并数据源add之后：$lineageMap")
  }

  /**
   * merge相同数据源的对象
   */
  private[fire] def mergeDatasource(datasourceList: JHashSet[DatasourceDesc], datasourceDesc: DatasourceDesc): JHashSet[DatasourceDesc] = {
    ConnectorParserManager.merge(datasourceList, datasourceDesc)
  }

  /**
   * 向队列中添加一条sql类型的数据源，用于后续异步解析
   */
  private[fire] def addDBSqlSource(source: DBSqlSource): Unit = {
    if (lineageEnable && this.addDBCount.incrementAndGet() <= lineageMaxSize) this.dbSqlQueue.offer(source)
  }

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def get: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = this.lineageMap
}

/**
 * 对外暴露API，用于收集并处理各种埋点信息
 */
object LineageManager extends Logging {
  private[fire] lazy val manager = new LineageManager
  private lazy val sparkSqlParser = "com.zto.fire.spark.sql.SparkSqlParser"
  private lazy val flinkSqlParser = "com.zto.fire.flink.sql.FlinkSqlParser"
  private lazy val sparkLineageAccumulatorManager = "com.zto.fire.spark.sync.SparkLineageAccumulatorManager"
  private lazy val flinkLineageAccumulatorManager = "com.zto.fire.flink.sync.FlinkLineageAccumulatorManager"

  /**
   * 向标准输出流打印血缘日志
   * 注：仅用于debug协助问题定位
   *
   * @param msg
   * 日志内容
   */
  private[fire] def printLog(msg: String): Unit = {
    if (lineageDebugEnable) {
      val log = s"lineage=>$msg"
      logInfo(log)
      println(log)
    }
  }

  /**
   * 周期性的打印血缘json到master端
   *
   * @param interval
   * 时间间隔
   */
  def print(interval: Long = 60, pretty: Boolean = true): Unit = {
    ThreadUtils.schedule({
      val lineage = FireUtils.invokeEngineApi[Lineage](this.sparkLineageAccumulatorManager, this.flinkLineageAccumulatorManager, "getValue")
      val jsonLineage = FirePS1Conf.wrap(
        s"""
           |------------------- 血缘信息（${DateFormatUtils.formatCurrentDateTime()}）：----------------------
           |${JSONUtils.toJSONString(lineage, pretty)}
           |""".stripMargin, FirePS1Conf.PINK)

      println(jsonLineage)
      logInfo(jsonLineage)
    }, interval, interval, timeUnit = TimeUnit.SECONDS)
  }

  /**
   * 周期性的打印血缘json到master端
   *
   * @param interval
   * 时间间隔
   */
  def show(interval: Long = 60): Unit = this.print(interval)


  /**
   * 添加一条sql记录到队列中
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群信息
   * @param username
   * 用户名
   * @param sql
   * 待解析的sql语句
   */
  private[fire] def addDBSql(datasource: Datasource, cluster: String, username: String, sql: String, operations: JHashSet[Operation]): Unit = {
    this.manager.addDBSqlSource(DBSqlSource(datasource.toString, cluster, username, sql, operations))
  }

  /**
   * 添加一条sql记录到队列中
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群信息
   * @param username
   * 用户名
   * @param sql
   * 待解析的sql语句
   */
  private[fire] def addDBSql(datasource: Datasource, cluster: String, username: String, sql: String, operation: Operation): Unit = {
    val operations = new JHashSet[Operation]()
    operations.add(operation)
    this.addDBSql(datasource, cluster, username, sql, operations)
  }

  /**
   * 根据SQL血缘解析的Hive、Hudi表信息添加到数据源中
   *
   * @param tables
   * SQLTable实例，来自于sql中的血缘解析
   */
  private def mapTableToDatasource(tables: JList[SQLTable]): Unit = {
    tryWithLog {
      tables.filter(_ != null).foreach(table => {
        LineageManager.printLog("1. 开始将SQLTable中的血缘信息合并到Datasource中")
        val connector = if (noEmpty(table.getConnector)) table.getConnector else table.getCatalog
        val datasourceClass = Datasource.toDatasource(Datasource.parse(connector))

        if (datasourceClass != null) {
          val method = ReflectionUtils.getMethodByName(datasourceClass, "mapDatasource")
          if (method != null) {
            LineageManager.printLog(s"2. 开始调用类：${datasourceClass}的方法：${method.getName} connector：${connector}")
            method.invoke(null, table)
          } else {
            LineageManager.printLog(s"类：${datasourceClass}中未定义mapDatasource()方法，请检查！")
          }
        } else if (!"view".equalsIgnoreCase(connector)) {
          val log = s"未找到匹配的connector：${connector}，无法将SQLTable映射为Datasource，请检查Datasource中静态代码块映射关系！"
          logWarning(log)
          LineageManager.printLog(log)
        }
      })
    }(this.logger, catchLog = "将SQLTable血缘信息映射为Datasource数据源信息失败！", isThrow = false, hook = false)
  }

  /**
   * 添加数据源信息
   *
   * @param sourceType
   * 数据源类型
   * @param datasourceDesc
   * 数据源描述
   */
  private[fire] def addDatasource(sourceType: Datasource, datasourceDesc: DatasourceDesc): Unit = {
    this.manager.add(sourceType, datasourceDesc)
  }

  /**
   * 添加数据源信息
   *
   * @param datasourceDesc
   * 数据源描述
   * @param operations
   * 操作类型
   */
  def addLineage(datasourceDesc: DatasourceDesc, operations: Operation*): Unit = {
    if (isEmpty(datasourceDesc)) return

    this.addDatasource(this.getDatasourceType(datasourceDesc), this.mergeOperations(datasourceDesc, operations: _*))
  }

  // ----------------------------------------------- SQL数据源 ---------------------------------------------------------- //

  /**
   * 用于采集计算引擎中执行的sql语句
   *
   * @param sql
   * sql 脚本
   */
  def addSql(sql: String): Unit = {
    if (isEmpty(sql)) return

    FireUtils.invokeEngineApi[Unit](this.sparkSqlParser, this.flinkSqlParser, "sqlParse", Array[Class[_]](classOf[String]), Array[Object](sql))
    printLog(s"引擎主动调用采集SQL：$sql")
  }

  /**
   * 用于采集计算引擎中执行的sql语句
   *
   * @param sql
   * sql 脚本
   */
  def addSqlLineage(sql: String): Unit = this.addSql(sql)

  // ----------------------------------------------- 数仓数据源 ---------------------------------------------------------- //

  /**
   * 添加Hive数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addHiveLineage(tableName: String, operations: Operation*): Unit = {
    this.addLineage(HiveDatasource(Datasource.HIVE.toString, FireHiveConf.hiveCluster, tableName, null, null), operations: _*)
  }

  /**
   * 添加Hudi数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addHudiLineage(tableName: String, cluster: String, tableType: String, recordKey: String, precombineKey: String, partitionField: String, operations: Operation*): Unit = {
    this.addLineage(HudiDatasource(Datasource.HUDI.toString, cluster, tableName, tableType, recordKey, precombineKey, partitionField), operations: _*)
  }

  /**
   * 添加FileSystem数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addFileSystemLineage(datasource: Datasource, cluster: String, tableName: String,
                           partitions: JSet[SQLTablePartitions], operations: Operation*): Unit = {
    this.addLineage(FileDatasource(datasource.toString, cluster, tableName, partitions), operations: _*)
  }


  /**
   * 添加Iceberg数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addIcebergLineage(cluster: String, tableName: String, operations: Operation*): Unit = {
    this.addFileSystemLineage(Datasource.ICEBERG, cluster, tableName, null, operations: _*)
  }

  /**
   * 添加Paimon数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addPaimonLineage(cluster: String, tableName: String, primaryKey: String,
                       bucketKey: String, partitionField: String, operations: Operation*): Unit = {
    this.addLineage(PaimonDatasource(Datasource.PAIMON.toString, cluster, tableName, primaryKey, bucketKey, partitionField), operations: _*)
  }

  /**
   * 添加Dynamodb数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDynamodbLineage(cluster: String, tableName: String, operations: Operation*): Unit = {
    this.addFileSystemLineage(Datasource.DYNAMODB, cluster, tableName, null, operations: _*)
  }

  /**
   * 添加Firehose数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addFirehoseLineage(cluster: String, tableName: String, operations: Operation*): Unit = {
    this.addFileSystemLineage(Datasource.FIREHOSE, cluster, tableName, null, operations: _*)
  }

  /**
   * 添加Kinesis数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addKinesisLineage(cluster: String, tableName: String, operations: Operation*): Unit = {
    this.addFileSystemLineage(Datasource.KINESIS, cluster, tableName, null, operations: _*)
  }

  // ----------------------------------------------- 虚拟数据源 ---------------------------------------------------------- //

  /**
   * 添加datagen数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDataGenLineage: Unit = {
    this.addLineage(VirtualDatasource(Datasource.DATAGEN.toString), Operation.SOURCE)
  }

  /**
   * 添加print数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addPrintLineage: Unit = {
    this.addLineage(VirtualDatasource(Datasource.PRINT.toString), Operation.SINK)
  }

  /**
   * 添加BlackHole数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addBlackHoleLineage: Unit = {
    this.addLineage(VirtualDatasource(Datasource.BLACKHOLE.toString), Operation.SINK)
  }

  // ----------------------------------------------- 自定义数据源 ---------------------------------------------------------- //

  /**
   * 添加CustomizeSource数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addCustomizeSourceLineage(datasource: String, cluster: String, sourceType: String): Unit = {
    this.addLineage(CustomizeDatasource(datasource, cluster, sourceType), Operation.SOURCE)
  }

  /**
   * 添加CustomizeSink数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addCustomizeSinkLineage(datasource: String, cluster: String, sourceType: String): Unit = {
    this.addLineage(CustomizeDatasource(datasource, cluster, sourceType), Operation.SINK)
  }

  // ----------------------------------------------- 消息队列数据源 ---------------------------------------------------------- //

  /**
   * 添加Kafka数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addKafkaLineage(keyNum: Int, operations: Operation*): Unit = {
    lazy val (finalBrokers, finalTopic, _) = KafkaUtils.getConfByKeyNum(null, null, null, keyNum)
    this.addLineage(MQDatasource("kafka", finalBrokers, finalTopic, FireKafkaConf.kafkaGroupId(keyNum)), operations: _*)
  }


  /**
   * 添加Kafka数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addKafkaLineage2(cluster: String, topics: String, groupId: String, operations: Operation*): Unit = {
    this.addLineage(MQDatasource("kafka", cluster, topics, groupId), operations: _*)
  }

  /**
   * 添加RocketMQ数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addRocketMQLineage(keyNum: Int, operations: Operation*): Unit = {
    val (finalBrokers, finalTopic, _, _) = RocketMQUtils.getConfByKeyNum(null, null, null, null, keyNum)
    this.addLineage(MQDatasource("rocketmq", finalBrokers, finalTopic, FireRocketMQConf.rocketGroupId(keyNum)), operations: _*)
  }

  /**
   * 添加RocketMQ数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addRocketMQLineage2(cluster: String, topics: String, groupId: String, operations: Operation*): Unit = {
    this.addLineage(MQDatasource("rocketmq", cluster, topics, groupId), operations: _*)
  }

  // ----------------------------------------------- 数据库类型数据源 ---------------------------------------------------------- //

  /**
   * 添加数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDBLineage(dbType: Datasource, tableName: String, keyNum: Int, operations: Operation*): Unit = {
    val url = PropUtils.getString("db.jdbc.url", "", keyNum)
    val username = PropUtils.getString("db.jdbc.user", "", keyNum)
    this.addLineage(DBDatasource(dbType.toString, url, tableName, username), operations: _*)
  }

  /**
   * 添加数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDBLineage2(dbType: Datasource, cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addLineage(DBDatasource(dbType.toString, cluster, tableName, username), operations: _*)
  }

  /**
   * 添加HBase数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addHBaseLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.HBASE, PropUtils.getString("hbase.cluster", "", keyNum), tableName, PropUtils.getString("hbase.user", "", keyNum), operations: _*)
  }

  /**
   * 添加HBase数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addHBaseLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.HBASE, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addMySQLLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.MYSQL, tableName, keyNum, operations: _*)
  }

  /**
   * 添加数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addMySQLLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.MYSQL, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加tidb数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addTidbLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.TIDB, tableName, keyNum, operations: _*)
  }

  /**
   * 添加tidb数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addTidbLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.TIDB, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Oracle数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addOracleLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.ORACLE, tableName, keyNum, operations: _*)
  }

  /**
   * 添加Oracle数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addOracleLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.ORACLE, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Oracle数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addADBLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.ADB, tableName, keyNum, operations: _*)
  }

  /**
   * 添加Oracle数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addADBLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.ADB, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加DB2数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDB2Lineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.DB2, tableName, keyNum, operations: _*)
  }

  /**
   * 添加DB2数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDB2Lineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.DB2, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Clickhouse数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addClickhouseLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.CLICKHOUSE, tableName, keyNum, operations: _*)
  }

  /**
   * 添加Clickhouse数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addClickhouseLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.CLICKHOUSE, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加SQLServer数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addSQLServerLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.SQLSERVER, tableName, keyNum, operations: _*)
  }

  /**
   * 添加SQLServer数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addSQLServerLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.SQLSERVER, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Derby数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDerbyLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.DERBY, tableName, keyNum, operations: _*)
  }

  /**
   * 添加Derby数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDerbyLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.DERBY, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Influxdb数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addInfluxdbLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.INFLUXDB, tableName, keyNum, operations: _*)
  }

  /**
   * 添加Influxdb数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addInfluxdbLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.INFLUXDB, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Promethus数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addPromethusLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.PROMETHUS, tableName, keyNum, operations: _*)
  }

  /**
   * 添加Promethus数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addPromethusLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.PROMETHUS, cluster, tableName, username, operations: _*)
  }


  /**
   * 添加Presto数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addPrestoLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.PRESTO, tableName, keyNum, operations: _*)
  }

  /**
   * 添加Presto数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addPrestoLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.PRESTO, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Kylin数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addKylinLineage(tableName: String, keyNum: Int, operations: Operation*): Unit = {
    this.addDBLineage(Datasource.KYLIN, tableName, keyNum, operations: _*)
  }

  /**
   * 添加Kylin数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addKylinLineage2(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.KYLIN, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Redis数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addRedisLineage(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.REDIS, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加MongoDB数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addMongoDBLineage(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.MONGODB, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Doris数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDorisLineage(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.DORIS, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加ES数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addESLineage(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.ELASTICSEARCH, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加Doris数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addOpenSearchLineage(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.OPENSEARCH, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加StarRocks数据库数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addStarRocksLineage(cluster: String, tableName: String, username: String, operations: Operation*): Unit = {
    this.addDBLineage2(Datasource.STARROCKS, cluster, tableName, username, operations: _*)
  }

  /**
   * 添加接口数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addUrlLineage(datasource: Datasource, url: String, operations: Operation*): Unit = {
    this.addLineage(UrlDatasource(datasource.toString, url), operations: _*)
  }

  /**
   * 添加接口数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addHttpLineage(url: String, operations: Operation*): Unit = {
    this.addUrlLineage(Datasource.HTTP, url, operations: _*)
  }

  /**
   * 添加接口数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addRpcLineage(url: String, operations: Operation*): Unit = {
    this.addUrlLineage(Datasource.RPC, url, operations: _*)
  }

  /**
   * 添加Dubbo接口数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addDubboLineage(url: String, operations: Operation*): Unit = {
    this.addUrlLineage(Datasource.DUBBO, url, operations: _*)
  }

  /**
   * 添加Interface接口数据源信息
   *
   * @param operations
   * 操作类型
   */
  def addInterfaceLineage(url: String, operations: Operation*): Unit = {
    this.addUrlLineage(Datasource.INTERFACE, url, operations: _*)
  }

  /**
   * 为指定数据源添加Operation类型
   *
   * @param datasource
   * 数据源
   * @param targetOperations
   * 操作类型集合
   */
  @Internal
  private[this] def mergeOperations(datasource: DatasourceDesc, targetOperations: Operation*): DatasourceDesc = {
    if (isEmpty(datasource, targetOperations)) return datasource

    // 将target中的operation几盒添加到source数据源中
    val clazz = datasource.getClass
    val operationMethod = ReflectionUtils.getMethodByName(clazz, "operation")
    if (operationMethod != null) {
      val operation = operationMethod.invoke(datasource)
      val sourceOptions = if (operation != null) operation.asInstanceOf[JHashSet[Operation]] else new JHashSet[Operation]()
      if (operation != null) {
        val methodEq = ReflectionUtils.getMethodByName(clazz, "operation_$eq")
        if (methodEq != null) {
          sourceOptions.addAll(JavaConversions.setAsJavaSet(targetOperations.toSet))
          methodEq.invoke(datasource, sourceOptions)
        }
      }
    }
    datasource
  }

  /**
   * 获取Datasouce具体的数据源类型
   */
  @Internal
  private[this] def getDatasourceType(datasource: DatasourceDesc): Datasource = {
    if (isEmpty(datasource)) return Datasource.UNKNOWN
    val datasourceField = ReflectionUtils.getFieldByName(datasource.getClass, "datasource")
    if (isEmpty(datasourceField)) return Datasource.UNKNOWN

    Datasource.parse(datasourceField.get(datasource).toString)
  }

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def getDatasourceLineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = this.manager.get

  /**
   * 获取完整的实时血缘信息
   */
  private[fire] def getLineage: Lineage = {
    new Lineage(this.getDatasourceLineage, SQLLineageManager.getSQLLineage)
  }

  /**
   * 将目标DataSourceDesc中的operation合并到set中
   */
  private[fire] def mergeSet(set: JHashSet[DatasourceDesc], datasourceDesc: DatasourceDesc): Unit = this.synchronized {
    if (set.isEmpty || !set.contains(datasourceDesc)) {
      set.add(datasourceDesc)
      return
    }

    if (set.contains(datasourceDesc)) {
      set.foreach(ds => {
        if (ds.equals(datasourceDesc)) {
          // 反射调用case class中的operation进行set合并
          ConnectorParserManager.addOperation(datasourceDesc, ds)
        }
      })

      LineageManager.printLog(s"合并前血缘set集合：$set Datasource实例：$datasourceDesc")
      set.replace(datasourceDesc)
      LineageManager.printLog("合并后血缘set集合：" + set)
    }
  }

  /**
   * 合并两个血缘map
   *
   * @param current
   * 待合并的map
   * @param target
   * 目标map
   * @return
   * 合并后的血缘map
   */
  private[fire] def mergeLineageMap(current: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]], target: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = {
    printLog(s"1. 双血缘map合并 current：$current target：$target")
    target.foreach(ds => {
      val datasourceDesc = current.mergeGet(ds._1)(ds._2)
      if (ds._2.nonEmpty) {
        ds._2.foreach(desc => {
          current.put(ds._1, this.manager.mergeDatasource(datasourceDesc, desc))
        })
      }
    })
    printLog(s"2. 双血缘map合并 current：$current")
    current
  }
}


