package com.zto.fire.common.util

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.zto.fire.common.enu.DataSource

/**
 * 用于统计当前任务使用到的数据源信息，包括MQ、DB等连接信息等
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-11-26 15:30
 */
private[fire] class DataSourceManager {
  // 用于存放当前任务用到的数据源信息
  private[this] lazy val datasourceMap = new ConcurrentHashMap[DataSource, util.HashSet[DataSourceDesc]]()

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
  private[fire] def addDBDataSource(datasource: String, cluster: String, sink: Boolean, tableName: String, username: String = ""): Unit = {
    this.manager.add(DataSource.parse(datasource), DBDataSource(datasource, cluster, sink, tableName, username))
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
  private[fire] def addMQDataSource(datasource: String, cluster: String, sink: Boolean = false, topics: String, groupId: String = ""): Unit = {
    this.manager.add(DataSource.parse(datasource), MQDataSource(datasource, cluster, sink, topics, groupId))
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
case class DBDataSource(datasource: String, cluster: String, sink: Boolean, tableName: String, username: String) extends DataSourceDesc

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
case class MQDataSource(datasource: String, cluster: String, sink: Boolean = false, topics: String, groupId: String) extends DataSourceDesc