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

import com.zto.fire.common.bean.lineage.{Lineage, SQLTable}
import com.zto.fire.common.conf.FireFrameworkConf._
import com.zto.fire.common.enu.{Datasource, Operation, ThreadPoolType}
import com.zto.fire.common.lineage.parser.ConnectorParserManager
import com.zto.fire.common.lineage.parser.connector._
import com.zto.fire.common.util._
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

import java.util.concurrent._
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
  private var parseCount = 0
  // 用于收集各实时引擎执行的sql语句
  this.lineageParse()

  /**
   * 用于异步解析sql中使用到的表，并放到linageMap中
   */
  private[this] def lineageParse(): Unit = {
    if (lineageEnable) {
      this.parserExecutor.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = {
          parseCount += 1

          if (parseCount >= lineageRunCount && !parserExecutor.isShutdown) {
            logger.info(s"3. 异步解析实时血缘的定时任务采样共计：${lineageRunCount}次，即将退出异步线程")
            parserExecutor.shutdown()
          }

          // 1. 解析jdbc sql语句
          val start = currentTime
          parseJdbcSql()

          // 2. 将SQL中的hive表映射到数据源结构中
          LineageManager.addSQLTables(SQLLineageManager.getSQLLineage.getTables)
          logger.info(s"2. 完成第${parseCount}/${lineageRunCount}次异步解析SQL埋点中的表信息，耗时：${elapsed(start)}")
        }
      }, lineageRunInitialDelay, lineageRunPeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 解析来自于jdbc的sql血缘
   */
  private[this] def parseJdbcSql(): Unit = {
    tryWithLog {
      for (_ <- 1 until dbSqlQueue.size()) {
        val sqlSource = dbSqlQueue.poll()
        if (sqlSource != null) {
          val tableNames = SQLUtils.tableParse(sqlSource.sql)
          if (tableNames != null && tableNames.nonEmpty) {
            tableNames.filter(StringUtils.isNotBlank).foreach(tableName => {
              add(Datasource.parse(sqlSource.datasource), DBDatasource(sqlSource.datasource, sqlSource.cluster, tableName, sqlSource.username, operation = sqlSource.operation))
            })
          }
        }
      }
    }(logger, s"1. 开始第${parseCount}/${lineageRunCount}次解析JDBC中的血缘信息", "jdbc血缘信息解析失败")
  }

  /**
   * 添加一个数据源描述信息
   */
  private[fire] def add(sourceType: Datasource, datasourceDesc: DatasourceDesc): Unit = {
    if (!lineageEnable || this.lineageMap.size() > lineMaxSize) return
    val set = this.lineageMap.mergeGet(sourceType)(new JHashSet[DatasourceDesc]())
    if (set.isEmpty) set.add(datasourceDesc)
    val mergedSet = this.mergeDatasource(set, datasourceDesc)
    this.lineageMap.put(sourceType, mergedSet)
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
  private[fire] def addDBSqlSource(source: DBSqlSource): Unit = if (lineageEnable && this.dbSqlQueue.size() <= lineMaxSize) this.dbSqlQueue.offer(source)

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def get: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = this.lineageMap
}

/**
 * 对外暴露API，用于收集并处理各种埋点信息
 */
private[fire] object LineageManager extends Logging {
  private[fire] lazy val manager = new LineageManager

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
   * 添加多个数据源操作
   */
  private[fire] def toOperationSet(operation: Operation*): JHashSet[Operation] = {
    val operationSet = new JHashSet[Operation]
    operation.foreach(operationSet.add)
    operationSet
  }

  /**
   * 根据SQL血缘解析的Hive、Hudi表信息添加到数据源中
   *
   * @param tables
   * SQLTable实例，来自于sql中的血缘解析
   */
  def addSQLTables(tables: JList[SQLTable]): Unit = {
    // 将hive表映射到datasource血缘列表中
    tables.filter(tab => "hive".equalsIgnoreCase(tab.getCatalog)).foreach(table => {
      val operations = new JHashSet[Operation]()
      table.getOperation.map(t => operations.add(Operation.parse(t)))
      this.addDatasource(Datasource.HIVE, HiveDatasource(Datasource.HIVE.toString, table.getCluster, table.getPhysicalTable, table.getPartitions, operations))
    })

    tables.filter(tab => "hudi".equalsIgnoreCase(tab.getCatalog)).foreach(table => {
      val operations = new JHashSet[Operation]()
      table.getOperation.map(t => operations.add(Operation.parse(t)))
      this.addDatasource(Datasource.HUDI, HudiDatasource(Datasource.HUDI.toString, table.getCluster, table.getPhysicalTable, operation = operations))
    })
  }

  /**
   * 添加数据源信息
   *
   * @param sourceType
   * 数据源类型
   * @param datasourceDesc
   * 数据源描述
   */
  def addDatasource(sourceType: Datasource, datasourceDesc: DatasourceDesc): Unit = {
    this.manager.add(sourceType, datasourceDesc)
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
   * 合并两个血缘map
   *
   * @param current
   * 待合并的map
   * @param target
   * 目标map
   * @return
   * 合并后的血缘map
   */
  def mergeLineageMap(current: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]], target: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = {
    target.foreach(ds => {
      val datasourceDesc = current.mergeGet(ds._1)(ds._2)
      if (ds._2.nonEmpty) {
        ds._2.foreach(desc => {
          current.put(ds._1, this.manager.mergeDatasource(datasourceDesc, desc))
        })
      }
    })
    current
  }
}


