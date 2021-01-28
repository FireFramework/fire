package com.zto.fire.common.util

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}

import com.google.common.collect.EvictingQueue
import com.zto.fire.common.conf.FireFrameworkConf._
import com.zto.fire.common.enu.{DataSource, ThreadPoolType}
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
 * 用于统计当前任务使用到的数据源信息，包括MQ、DB等连接信息等
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-11-26 15:30
 */
private[fire] class DataSourceManager {
  private[this] lazy val logger = LoggerFactory.getLogger(this.getClass)
  // 用于存放当前任务用到的数据源信息
  private[this] lazy val datasourceMap = new ConcurrentHashMap[DataSource, util.HashSet[DataSourceDesc]]()
  // 用于收集来自不同数据源的sql语句，后续会异步进行SQL解析，考虑到分布式场景下会有很多重复的SQL执行，因此使用了线程不安全的队列即可满足需求
  private lazy val sqlQueue = EvictingQueue.create[DBSqlSource](buriedPointDatasourceMaxSize)
  private[this] lazy val threadPool = ThreadUtils.createThreadPool("DataSourceManager", ThreadPoolType.SCHEDULED)
  this.sqlParse()

  /**
   * 用于异步解析sql中使用到的表，并放到datasourceMap中
   */
  private[this] def sqlParse(): Unit = {
    if (buriedPointDatasourceEnable && threadPool != null) {
      threadPool.asInstanceOf[ScheduledExecutorService].scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = {
          val start = currentTime
          if (sqlQueue != null) {
            for (i <- 1 until sqlQueue.size()) {
              val sqlSource = sqlQueue.poll()
              if (sqlSource != null) {
                val tableNames = SQLUtils.tableParse(sqlSource.sql)
                if (tableNames != null && tableNames.nonEmpty) {
                  tableNames.filter(StringUtils.isNotBlank).foreach(tableName => {
                    add(DataSource.parse(sqlSource.datasource), DBDataSource(sqlSource.datasource, sqlSource.cluster, tableName, sqlSource.username, sqlSource.sink))
                  })
                }
              }
            }
            logger.info(s"异步解析SQL埋点中的表信息,耗时：${timecost(start)}")
          }
        }
      }, buriedPointDatasourceInitialDelay, buriedPointDatasourcePeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 添加一个数据源描述信息
   */
  private[fire] def add(sourceType: DataSource, datasourceDesc: DataSourceDesc): Unit = {
    var set = this.datasourceMap.get(sourceType)
    if (set == null) {
      set = new util.HashSet[DataSourceDesc]()
    }
    set.add(datasourceDesc)
    this.datasourceMap.put(sourceType, set)
  }

  /**
   * 向队列中添加一条sql类型的数据源，用于后续异步解析
   */
  private[fire] def addSql(source: DBSqlSource): Unit = if (buriedPointDatasourceEnable) this.sqlQueue.offer(source)

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def get: util.Map[DataSource, util.HashSet[DataSourceDesc]] = this.datasourceMap
}

/**
 * 对外暴露API，用于收集并处理各种埋点信息
 */
private[fire] object DataSourceManager {
  private lazy val manager = new DataSourceManager

  /**
   * 添加一条sql记录到队列中
   *
   * @param datasource
   *             数据源类型
   * @param cluster
   *             集群信息
   * @param sink source or sink
   * @param username
   *             用户名
   * @param sql
   *             待解析的sql语句
   */
  private[fire] def addSql(datasource: String, cluster: String, username: String, sql: String, sink: Boolean = true): Unit = {
    this.manager.addSql(DBSqlSource(datasource, cluster, username, sql, sink))
  }

  /**
   * 添加一条DB的埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群信息
   * @param sink
   * source or sink
   * @param tableName
   * 表名
   * @param username
   * 连接用户名
   */
  private[fire] def addDBDataSource(datasource: String, cluster: String, tableName: String, username: String = "", sink: Boolean = true): Unit = {
    this.manager.add(DataSource.parse(datasource), DBDataSource(datasource, cluster, tableName, username, sink))
  }

  /**
   * 添加一条MQ的埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群标识
   * @param sink
   * product or consumer
   * @param topics
   * 主题列表
   * @param groupId
   * 消费组标识
   */
  private[fire] def addMQDataSource(datasource: String, cluster: String, topics: String, groupId: String, sink: Boolean = false): Unit = {
    this.manager.add(DataSource.parse(datasource), MQDataSource(datasource, cluster, topics, groupId, sink))
  }

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def get: util.Map[DataSource, util.HashSet[DataSourceDesc]] = this.manager.get
}

/**
 * 数据源描述
 */
trait DataSourceDesc

/**
 * 面向数据库类型的数据源，带有tableName
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param sink
 * true: sink false: source
 * @param tableName
 * 表名
 * @param username
 * 使用关系型数据库时作为jdbc的用户名，HBase留空
 */
case class DBDataSource(datasource: String, cluster: String, tableName: String, username: String = "", sink: Boolean = true) extends DataSourceDesc

/**
 * 面向数据库类型的数据源，需将SQL中的tableName主动解析
 *
 * @param datasource
 *            数据源类型，参考DataSource枚举
 * @param cluster
 *            数据源的集群标识
 * @param sink
 *            true: sink false: source
 * @param username
 *            使用关系型数据库时作为jdbc的用户名，HBase留空
 * @param sql 执行的SQL语句
 */
case class DBSqlSource(datasource: String, cluster: String, username: String, sql: String, sink: Boolean = true) extends DataSourceDesc

/**
 * MQ类型数据源，如：kafka、RocketMQ等
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param sink
 * true: sink false: source
 * @param topics
 * 使用到的topic列表
 * @param groupId
 * 任务的groupId
 */
case class MQDataSource(datasource: String, cluster: String, topics: String, groupId: String, sink: Boolean = false) extends DataSourceDesc