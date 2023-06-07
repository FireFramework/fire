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

package com.zto.fire.core.conf

import com.google.common.collect.Sets
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.conf.FireFrameworkConf.FIRE_LOG_SQL_LENGTH
import com.zto.fire.common.conf.FireKafkaConf._
import com.zto.fire.common.conf.FireRocketMQConf._
import com.zto.fire.common.conf.{FireHDFSConf, FireHiveConf, KeyNum}
import com.zto.fire.common.util.{Logging, PropUtils, ReflectionUtils, StringsUtils}
import com.zto.fire.core.BaseFire
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.{Handle, Process, Step1, Step10, Step11, Step12, Step13, Step14, Step15, Step16, Step17, Step18, Step19, Step2, Step3, Step4, Step5, Step6, Step7, Step8, Step9}
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

import java.lang.annotation.Annotation
import scala.collection.mutable.HashMap


/**
 * 注解管理器：用于将主键中的配置信息映射为键值对信息
 * 注：解析指定的配置注解需要满足以下两个条件：
 *  1. 在registerAnnoSet中注册新的注解
 *     2. 开发对应的map方法，如：mapHive解析@Hive、mapKafka解析@kafka注解
 *
 * @author ChengLong 2022-04-26 11:19:00
 * @since 2.2.2
 */
@Internal
private[fire] trait AnnoManager extends Logging {
  protected[fire] lazy val props = new HashMap[String, String]()


  this.register

  /**
   * 用于注册需要映射配置信息的自定义主键
   */
  @Internal
  protected[fire] def register: Unit

  /**
   * 将键值对配置信息存放到map中
   *
   * @param key
   * 配置的key
   * @param value
   * 配置的value
   * @param keyNum
   * 配置key的数字结尾标识
   */
  @Internal
  protected def put(key: String, value: Any, keyNum: Int = KeyNum._1): this.type = {
    if (noEmpty(key, value)) {
      // 将配置中多余的空格去掉
      val fixKey = StringUtils.trim(key)
      val fixValue = StringUtils.trim(value.toString)

      // 如果keyNum>1则将数值添加到key的结尾
      val realKey = if (keyNum > 1) fixKey + keyNum else fixKey
      val isNumeric = StringsUtils.isNumeric(fixValue)

      // 约定注解中指定的配置的值如果为-1，表示不使用该项配置，通常-1表示默认值
      if (!isNumeric || (isNumeric && fixValue.toLong != -1)) {
        this.props.put(realKey, fixValue)
      }
    }
    this
  }

  /**
   * 解析并将配置放入指定配置前缀的conf中
   *
   * @param configPrefix
   * fire中定义的key的统一前缀
   * @param config
   * 多个配置，同一行中的key value以等号分隔
   */
  @Internal
  private def putConfig(configPrefix: String, config: Array[String], keyNum: Int = KeyNum._1): Unit = {
    if (noEmpty(configPrefix, config)) {
      config.foreach(conf => {
        val kv = conf.split("=")
        if (kv != null && kv.length == 2) {
          this.put(s"${configPrefix}${kv(0).trim}", kv(1).trim, keyNum)
        }
      })
    }
  }

  /**
   * 获取主键转为key value形式的Properties对象
   */
  @Internal
  def getAnnoProps(baseFire: Class[_]): HashMap[String, String] = {
    if (baseFire == null) return this.props

    // 获取入口类上所有的注解
    val annotations = baseFire.getAnnotations
    val mapMethods = ReflectionUtils.getAllMethods(this.getClass)

    // 仅获取注册表中的注解配置信息
    annotations.filter(anno => AnnoManager.registerAnnoSet.contains(anno.annotationType())).foreach(anno => {
      // 反射调用map+注解名称对应的方法：
      // 比如注解名称为Hive，则调用mapHive方法解析@Hive注解中的配置信息
      val methodName = s"map${anno.annotationType().getSimpleName}"
      if (mapMethods.containsKey(methodName)) {
        mapMethods.get(methodName).invoke(this, anno)
      }
    })

    this.props
  }

  /**
   * 用于映射Hbase相关配置信息
   *
   * @param value
   * 对应注解中的value
   * @param config
   * 对应注解中的config
   */
  @Internal
  private def mapHBaseConf(value: String, cluster: String, family: String, batchSize: Int,
                           scanPartitions: Int, storageLevel: String, maxRetries: Int, durability: String,
                           tableMetaCache: Boolean, config: Array[String], keyNum: Int = KeyNum._1): Unit = {

    this.put("hbase.cluster", value, keyNum)
    this.put("hbase.cluster", cluster, keyNum)
    this.put("hbase.column.family", family, keyNum)
    this.put("fire.hbase.batch.size", batchSize, keyNum)
    this.put("fire.hbase.scan.partitions", scanPartitions, keyNum)
    this.put("fire.hbase.storage.level", storageLevel, keyNum)
    this.put("hbase.max.retry", maxRetries, keyNum)
    this.put("hbase.durability", durability, keyNum)
    this.put("fire.hbase.table.exists.cache.enable", tableMetaCache, keyNum)
    this.putConfig("fire.hbase.conf.", config, keyNum)
  }

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase
   * HBase注解实例
   */
  @Internal
  def mapHBase(hbase: HBase): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._1)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase2
   * HBase注解实例
   */
  @Internal
  def mapHBase2(hbase: HBase2): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._2)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase3
   * HBase注解实例
   */
  @Internal
  def mapHBase3(hbase: HBase3): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._3)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase4
   * HBase注解实例
   */
  @Internal
  def mapHBase4(hbase: HBase4): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._4)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase5
   * HBase注解实例
   */
  @Internal
  def mapHBase5(hbase: HBase5): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._5)

  /**
   * 用于映射JDBC相关配置信息
   * 对应注解中的@Jdbc
   */
  @Internal
  def mapJdbcConf(url: String, driver: String, username: String, password: String, isolationLevel: String,
                  maxPoolSize: Int, minPoolSize: Int, initialPoolSize: Int, acquireIncrement: Int,
                  maxIdleTime: Int, batchSize: Int, flushInterval: Long, maxRetries: Int, storageLevel: String,
                  queryPartitions: Int, logSqlLength: Int, connectionTimeout: Int, config: Array[String], keyNum: Int = KeyNum._1): Unit = {
    this.put("db.jdbc.url", url, keyNum)
    // TODO: driver自动推断
    // val autoDriver = if (noEmpty(driver)) driver else DBUtils
    this.put("db.jdbc.driver", driver, keyNum)
    this.put("db.jdbc.user", username, keyNum)
    this.put("db.jdbc.password", password, keyNum)
    this.put("db.jdbc.isolation.level", isolationLevel, keyNum)
    this.put("db.jdbc.maxPoolSize", maxPoolSize, keyNum)
    this.put("db.jdbc.minPoolSize", minPoolSize, keyNum)
    this.put("db.jdbc.initialPoolSize", initialPoolSize, keyNum)
    this.put("db.jdbc.acquireIncrement", acquireIncrement, keyNum)
    this.put("db.jdbc.maxIdleTime", maxIdleTime, keyNum)
    this.put("db.jdbc.batch.size", batchSize, keyNum)
    this.put("db.jdbc.flushInterval", flushInterval, keyNum)
    this.put("db.jdbc.max.retry", maxRetries, keyNum)
    this.put("fire.jdbc.storage.level", storageLevel, keyNum)
    this.put("fire.jdbc.query.partitions", queryPartitions, keyNum)
    this.put("db.jdbc.connection.timeout", connectionTimeout, keyNum)
    this.put(FIRE_LOG_SQL_LENGTH, logSqlLength, keyNum)

    this.putConfig("db.c3p0.conf.", config, keyNum)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi(hudi: Hudi): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._1))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._1))
    this.hudiParallelism(hudi.parallelism(), KeyNum._1)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._1)
    this.hudiClusterConf(hudi.clusterCommits(), hudi.clusterSchedule(), KeyNum._1)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi2(hudi: Hudi2): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._2))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._2))
    this.hudiParallelism(hudi.parallelism(), KeyNum._2)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._2)
    this.hudiClusterConf(hudi.clusterCommits(), hudi.clusterSchedule(), KeyNum._2)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi3(hudi: Hudi3): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._3))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._3))
    this.hudiParallelism(hudi.parallelism(), KeyNum._3)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._3)
    this.hudiClusterConf(hudi.clusterCommits(), hudi.clusterSchedule(), KeyNum._3)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi4(hudi: Hudi4): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._4))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._4))
    this.hudiParallelism(hudi.parallelism(), KeyNum._4)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._4)
    this.hudiClusterConf(hudi.clusterCommits(), hudi.clusterSchedule(), KeyNum._4)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi5(hudi: Hudi5): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._5))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._5))
    this.hudiParallelism(hudi.parallelism(), KeyNum._5)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._5)
    this.hudiClusterConf(hudi.clusterCommits(), hudi.clusterSchedule(), KeyNum._5)
  }

  /**
   * 统一设置hudi任务各项参数的并行度
   *
   * @param parallelism
   * 并行度
   */
  @Internal
  private[this] def hudiParallelism(parallelism: Int, keyNum: Int): Unit = {
    if (parallelism > 0) {
      this.toHudiConf(("hoodie.bloom.index.parallelism", parallelism.toString), keyNum)
      this.toHudiConf(("hoodie.simple.index.parallelism", parallelism.toString), keyNum)
      this.toHudiConf(("hoodie.insert.shuffle.parallelism", parallelism.toString), keyNum)
      this.toHudiConf(("hoodie.upsert.shuffle.parallelism", parallelism.toString), keyNum)
      this.toHudiConf(("hoodie.bulkinsert.shuffle.parallelism", parallelism.toString), keyNum)
      this.toHudiConf(("hoodie.delete.shuffle.parallelism", parallelism.toString), keyNum)
      this.toHudiConf(("hoodie.markers.delete.parallelism", parallelism.toString), keyNum)
      this.toHudiConf(("hoodie.rollback.parallelism", parallelism.toString), keyNum)
    }
  }

  /**
   * 用于配置hudi任务的compaction参数
   */
  @Internal
  private[this] def hudiClusterConf(clusterCommits: Int, clusterSchedule: Boolean, keyNum: Int): Unit = {
    if (clusterCommits > 0) {
      this.toHudiConf(("hoodie.clustering.inline.max.commits", clusterCommits.toString), keyNum)

      if (clusterSchedule) {
        this.toHudiConf(("hoodie.clustering.inline", "false"), keyNum)
        this.toHudiConf(("hoodie.clustering.schedule.inline", "true"), keyNum)
      } else {
        this.toHudiConf(("hoodie.clustering.inline", "true"), keyNum)
        this.toHudiConf(("hoodie.clustering.schedule.inline", "false"), keyNum)
      }
    }
  }

  /**
   * 用于配置hudi任务的compaction参数
   */
  @Internal
  private[this] def hudiCompactConf(compactCommits: Int, compactSchedule: Boolean, keyNum: Int): Unit = {
    if (compactCommits > 0) {
      this.toHudiConf(("hoodie.compact.inline.max.delta.commits", compactCommits.toString), keyNum)

      if (compactSchedule) {
        this.toHudiConf(("hoodie.compact.inline", "false"), keyNum)
        this.toHudiConf(("hoodie.compact.schedule.inline", "true"), keyNum)
      } else {
        this.toHudiConf(("hoodie.compact.inline", "true"), keyNum)
        this.toHudiConf(("hoodie.compact.schedule.inline", "false"), keyNum)
      }
    }
  }

  /**
   * 将配置转换为hudi的参数
   */
  private[this] def toHudiConf(kv: (String, String), keyNum: Int): Unit = {
    this.put("hudi.options." + kv._1, kv._2, keyNum)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc(jdbc: Jdbc): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._1)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc2(jdbc: Jdbc2): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._2)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc3
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc3(jdbc: Jdbc3): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._3)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc4
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc4(jdbc: Jdbc4): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._4)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc5
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc5(jdbc: Jdbc5): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._5)
  }

  /**
   * 用于映射Kafka相关配置信息
   */
  @Internal
  private def mapKafkaConf(brokers: String, topics: String, groupId: String, startingOffset: String,
                           endingOffsets: String, autoCommit: Boolean, sessionTimeout: Long, requestTimeout: Long,
                           pollInterval: Long, startFromTimestamp: Long, startFromGroupOffsets: Boolean,
                           forceOverwriteStateOffset: Boolean, forceAutoCommit: Boolean, forceAutoCommitInterval: Long,
                           config: Array[String], keyNum: Int = KeyNum._1
                          ): Unit = {

    this.put(KAFKA_BROKERS_NAME, brokers, keyNum)
    this.put(KAFKA_TOPICS, topics, keyNum)
    this.put(KAFKA_GROUP_ID, groupId, keyNum)
    this.put(KAFKA_STARTING_OFFSET, startingOffset, keyNum)
    this.put(KAFKA_ENDING_OFFSET, endingOffsets, keyNum)
    this.put(KAFKA_ENABLE_AUTO_COMMIT, autoCommit, keyNum)
    this.put(KAFKA_SESSION_TIMEOUT_MS, sessionTimeout, keyNum)
    this.put(KAFKA_REQUEST_TIMEOUT_MS, requestTimeout, keyNum)
    this.put(KAFKA_MAX_POLL_INTERVAL_MS, pollInterval, keyNum)
    this.put(KAFKA_START_FROM_TIMESTAMP, startFromTimestamp, keyNum)
    this.put(KAFKA_START_FROM_GROUP_OFFSETS, startFromGroupOffsets, keyNum)
    this.put(KAFKA_OVERWRITE_STATE_OFFSET, forceOverwriteStateOffset, keyNum)
    this.put(KAFKA_FORCE_AUTO_COMMIT, forceAutoCommit, keyNum)
    this.put(KAFKA_FORCE_AUTO_COMMIT_INTERVAL, forceAutoCommitInterval, keyNum)
    this.putConfig(kafkaConfStart, config, keyNum)
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka
   * Kafka注解实例
   */
  @Internal
  def mapKafka(kafka: Kafka): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.config(), KeyNum._1
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka2
   * Kafka注解实例
   */
  @Internal
  def mapKafka2(kafka: Kafka2): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.config(), KeyNum._2
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka3
   * Kafka注解实例
   */
  @Internal
  def mapKafka3(kafka: Kafka3): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.config(), KeyNum._3
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka4
   * Kafka注解实例
   */
  @Internal
  def mapKafka4(kafka: Kafka4): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.config(), KeyNum._4
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka5
   * Kafka注解实例
   */
  @Internal
  def mapKafka5(kafka: Kafka5): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.config(), KeyNum._5
    )
  }

  /**
   * 将@RocketMQ中配置的信息映射为键值对形式
   *
   * @param RocketMQ
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQConf(brokers: String, topics: String, groupId: String, consumerTag: String, startingOffset: String, autoCommit: Boolean, config: Array[String], keyNum: Int = KeyNum._1): Unit = {
    this.put(ROCKET_BROKERS_NAME, brokers, keyNum)
    this.put(ROCKET_TOPICS, topics, keyNum)
    this.put(ROCKET_GROUP_ID, groupId, keyNum)
    this.put(ROCKET_CONSUMER_TAG, consumerTag, keyNum)
    this.put(ROCKET_STARTING_OFFSET, startingOffset, keyNum)
    this.put(ROCKET_ENABLE_AUTO_COMMIT, autoCommit, keyNum)
    this.putConfig(rocketConfStart, config, keyNum)
  }

  /**
   * 将@RocketMQ中配置的信息映射为键值对形式
   *
   * @param RocketMQ
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ(rocketmq: RocketMQ): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.config, KeyNum._1)
  }

  /**
   * 将@RocketMQ2中配置的信息映射为键值对形式
   *
   * @param RocketMQ2
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ2(rocketmq: RocketMQ2): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.config, KeyNum._2)
  }

  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ3
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ3(rocketmq: RocketMQ3): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.config, KeyNum._3)
  }

  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ4
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ4(rocketmq: RocketMQ4): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.config, KeyNum._4)
  }

  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ5
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ5(rocketmq: RocketMQ5): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.config, KeyNum._5)
  }

  /**
   * 将@Hive中配置的信息映射为键值对形式
   *
   * @param Hive
   * Hive注解实例
   */
  @Internal
  def mapHive(hive: Hive): Unit = {
    if (noEmpty(hive.value())) this.put(FireHiveConf.HIVE_CLUSTER, hive.value())
    if (noEmpty(hive.cluster())) this.put(FireHiveConf.HIVE_CLUSTER, hive.cluster())
    if (noEmpty(hive.catalog())) this.put(FireHiveConf.HIVE_CATALOG_NAME, hive.catalog())
    if (noEmpty(hive.version())) this.put(FireHiveConf.HIVE_VERSION, hive.version())
    if (noEmpty(hive.partition())) this.put(FireHiveConf.DEFAULT_TABLE_PARTITION_NAME, hive.partition())

    // 加载hdfs相关参数（必须通过@Hive注解指定hive thrift的别名）
    if (noEmpty(hive.config())) {
      val hiveAlias = if (noEmpty(hive.cluster())) hive.cluster() else hive.value()
      if (FireHiveConf.hiveMetastoreMap.containsKey(hiveAlias)) {
        this.putConfig(FireHDFSConf.HDFS_HA_PREFIX + hiveAlias + ".", hive.config())
      }
    }
  }
}

object AnnoManager extends Logging {
  // 用于存放注册了的主键，只解析这些主键中的信息
  private[fire] lazy val registerAnnoSet = Sets.newHashSet[Class[_]](
    classOf[Hive], classOf[HBase], classOf[HBase2], classOf[HBase3], classOf[HBase4], classOf[HBase5],
    classOf[Jdbc], classOf[Jdbc2], classOf[Jdbc3], classOf[Jdbc4], classOf[Jdbc5], classOf[Kafka],
    classOf[Kafka2], classOf[Kafka3], classOf[Kafka4], classOf[Kafka5], classOf[RocketMQ], classOf[RocketMQ2],
    classOf[RocketMQ3], classOf[RocketMQ4], classOf[RocketMQ5], classOf[Hudi], classOf[Hudi2], classOf[Hudi3],
    classOf[Hudi4], classOf[Hudi5]
  )

  // 用于注册所有的生命周期注解
  private[fire] lazy val registerAnnoMethod = List[Class[_ <: Annotation]](classOf[Process], classOf[Handle],
    classOf[Step1], classOf[Step2], classOf[Step3], classOf[Step4], classOf[Step5], classOf[Step6], classOf[Step7],
    classOf[Step8], classOf[Step9], classOf[Step10], classOf[Step11], classOf[Step12], classOf[Step13], classOf[Step14],
    classOf[Step15], classOf[Step16], classOf[Step17], classOf[Step18], classOf[Step19])

  /**
   * 用于调起生命周期注解所标记的方法
   */
  protected[fire] def processAnno(baseFire: BaseFire): Unit = {
    tryWithLog {
      ReflectionUtils.invokeStepAnnoMethod(baseFire, this.registerAnnoMethod: _*)
    } (this.logger, "业务逻辑代码执行完成", "业务逻辑代码执行失败", isThrow = true)
  }

  /**
   * 用于调用指定的被注解标记的声明周期方法
   */
  protected[fire] def lifeCycleAnno(baseFire: BaseFire, annoClass: Class[_ <: Annotation]): Unit = {
    tryWithLog {
      ReflectionUtils.invokeAnnoMethod(baseFire, annoClass)
    } (this.logger, "生命周期方法调用成功", "生命周期方法调用失败", isThrow = true)
  }
}
