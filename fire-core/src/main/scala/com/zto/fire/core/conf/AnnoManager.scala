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
import com.zto.fire.core.anno.connector.{HBase11, HBase6, HBase7, HBase8, HBase9, Jdbc10, Jdbc11, Jdbc6, Jdbc7, Jdbc8, Jdbc9, Kafka10, Kafka11, Kafka6, Kafka7, Kafka8, Kafka9, RocketMQ10, RocketMQ11, RocketMQ4, RocketMQ6, RocketMQ7, RocketMQ8, RocketMQ9, _}
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
      val isNumeric = if (value.isInstanceOf[Int] || value.isInstanceOf[Long]) true else false

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
  private def mapHBaseConf(value: String, cluster: String, user: String, family: String, batchSize: Int,
                           scanPartitions: Int, storageLevel: String, maxRetries: Int, durability: String,
                           tableMetaCache: Boolean, config: Array[String], keyNum: Int = KeyNum._1): Unit = {

    this.put("hbase.cluster", value, keyNum)
    this.put("hbase.cluster", cluster, keyNum)
    this.put("hbase.user", user, keyNum)
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
  def mapHBase(hbase: HBase): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._1)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase2
   * HBase注解实例
   */
  @Internal
  def mapHBase2(hbase: HBase2): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._2)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase3
   * HBase注解实例
   */
  @Internal
  def mapHBase3(hbase: HBase3): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._3)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase4
   * HBase注解实例
   */
  @Internal
  def mapHBase4(hbase: HBase4): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._4)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase5
   * HBase注解实例
   */
  @Internal
  def mapHBase5(hbase: HBase5): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._5)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase6
   */
  @Internal
  def mapHBase6(hbase: HBase6): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._6)


  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase7
   */
  @Internal
  def mapHBase7(hbase: HBase7): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._7)


  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase8
   */
  @Internal
  def mapHBase8(hbase: HBase8): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._8)


  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase9
   */
  @Internal
  def mapHBase9(hbase: HBase9): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._9)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase10
   */
  @Internal
  def mapHBase10(hbase: HBase10): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._10)


  /**
   * 将@HBase中配置的信息映射为键值对形式
   *
   * @param HBase11
   */
  @Internal
  def mapHBase11(hbase: HBase11): Unit = this.mapHBaseConf(hbase.value(), hbase.cluster(), hbase.user(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), KeyNum._11)


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
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._1)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._1)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._1)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._1)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._1)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._1)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._1)
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
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._2)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._2)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._2)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._2)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._2)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._2)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._2)
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
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._3)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._3)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._3)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._3)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._3)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._3)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._3)
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
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._4)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._4)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._4)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._4)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._4)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._4)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._4)
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
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._5)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._5)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._5)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._5)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._5)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._5)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._5)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi6(hudi: Hudi6): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._6))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._6))
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._6)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._6)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._6)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._6)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._6)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._6)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._6)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi7(hudi: Hudi7): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._7))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._7))
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._7)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._7)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._7)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._7)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._7)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._7)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._7)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi8(hudi: Hudi8): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._8))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._8))
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._8)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._8)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._8)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._8)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._8)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._8)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._8)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi9(hudi: Hudi9): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._9))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._9))
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._9)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._9)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._9)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._9)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._9)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._9)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._9)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi10(hudi: Hudi10): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._10))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._10))
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._10)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._10)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._10)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._10)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._10)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._10)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._10)
  }

  /**
   * 将@Hudi中配置的信息映射为键值对形式
   *
   * @param hudi
   * Hudi注解实例
   */
  @Internal
  def mapHudi11(hudi: Hudi11): Unit = {
    // 解析通过注解配置的多个配置信息
    PropUtils.parseTextConfig(hudi.value()).foreach(kv => toHudiConf(kv, KeyNum._11))
    // 解析通过注解配置的单项配置信息
    hudi.props().map(conf => PropUtils.splitConfLine(conf)).filter(_.isDefined).map(_.get).foreach(kv => toHudiConf(kv, KeyNum._11))
    // 统一的并行度设置
    this.hudiParallelism(hudi.parallelism(), KeyNum._11)
    // 布隆索引相关设置
    this.hudiBloomIndexConf(hudi.bloomIndexParallelism(), hudi.useBloomIndexBucketized(), hudi.bloomkeysPerBucket, KeyNum._11)
    // 记录级索引相关设置
    this.hudiRecordIndexConf(hudi.useRecordIndex(), KeyNum._11)
    // hbase索引相关设置
    this.hudiHBaseIndexConf(hudi.useHBaseIndex(), hudi.hbaseZkQuorum(), hudi.hbasePort(), hudi.hbaseTable(), hudi.hbaseZkNodePath(), hudi.hbaseRollbackSync()
      , hudi.hbaseUpdatePartitionPath(), hudi.hbaseGetBatchSize(), hudi.hbasePutBatchSize(), hudi.hbasePutBatchSizeAutoCompute(), hudi.hbaseMaxQpsPerRegionServer()
      , hudi.hbaseQpsFraction(), hudi.hbaseQpsAllocatorClass(),hudi.hbaseIndexUser(), KeyNum._11)
    // clean相关设置
    this.hudiCleanConf(hudi.cleanerAsync(), hudi.cleanerPolicy(), hudi.cleanerCommitsRetained(), KeyNum._11)
    this.hudiCompactConf(hudi.compactCommits(), hudi.compactSchedule(), KeyNum._11)
    this.hudiClusteringConf(hudi.clusteringCommits(), hudi.clusteringSchedule(), hudi.clustringColumns(), hudi.clusteringPartitions(), KeyNum._11)
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
      this.toHudiConf(("hoodie.cleaner.parallelism", parallelism.toString), keyNum)
      this.toHudiConf(("hoodie.finalize.write.parallelism", parallelism.toString), keyNum)
    }
  }

  /**
   * 布隆过滤器参数调优
   *
   * @param parallelism
   * 并行度
   * @param useBloomIndexBucketized
   * 是否使用基于bucket的仿数据倾斜模式
   */
  @Internal
  private[this] def hudiBloomIndexConf(parallelism: Int, useBloomIndexBucketized: Boolean, bloomkeysPerBucket: Int, keyNum: Int): Unit = {
    if (parallelism > 0) {
      this.toHudiConf(("hoodie.bloom.index.parallelism", parallelism.toString), keyNum)
    }
    if (bloomkeysPerBucket > 0) {
      this.toHudiConf(("hoodie.bloom.index.keys.per.bucket", bloomkeysPerBucket.toString), keyNum)
    }
    this.toHudiConf(("hoodie.bloom.index.bucketized.checking", useBloomIndexBucketized.toString), keyNum)
  }

  /**
   * 是否开启记录级索引
   */
  @Internal
  private[this] def hudiRecordIndexConf(useRecordIndex: Boolean, keyNum: Int): Unit = {
    if (useRecordIndex) {
      this.toHudiConf(("hoodie.metadata.record.index.enable", useRecordIndex.toString), keyNum)
      this.toHudiConf(("hoodie.index.type", "RECORD_INDEX"), keyNum)
    }
  }

  /**
   * 是否开启hbase Index
   *
   * @param useHbaseIndex
   * @param hbaseZkQuorum
   * @param hbasePort
   * @param hbaseTable
   * @param hbaseZkNodePath
   * @param hbaseRollbackSync
   * @param hbaseUpdatePartitionPath
   * @param hbaseGetBatchSize
   * @param hbasePutBatchSize
   * @param hbasePutBatchSizeAutoCompute
   * @param hbaseMaxQpsPerRegionServer
   * @param hbaseQpsFraction
   * @param hbaseQpsAllocatorClass
   * @param keyNum
   */
  @Internal
  private[this] def hudiHBaseIndexConf(useHBaseIndex: Boolean, hbaseZkQuorum: String, hbasePort: Int
                                       , hbaseTable: String , hbaseZkNodePath: String, hbaseRollbackSync: Boolean
                                       , hbaseUpdatePartitionPath: Boolean, hbaseGetBatchSize: Long
                                       , hbasePutBatchSize: Long, hbasePutBatchSizeAutoCompute: Boolean
                                       , hbaseMaxQpsPerRegionServer: Long, hbaseQpsFraction: Float
                                       , hbaseQpsAllocatorClass: String, hbaseIndexUser:String, keyNum: Int) {
    if (useHBaseIndex) {
      requireNonEmpty(hbaseZkQuorum,hbaseTable){"Hudi Hbase Index需要指定zk集群地址和对应的hbase表名"}
      this.toHudiConf(("hoodie.index.type", "HBASE"), keyNum)
      this.toHudiConf(("hoodie.index.hbase.zkport", hbasePort.toString), keyNum)
      // 获取zk地址
      var zkUrl = hbaseZkQuorum
      if (noEmpty(zkUrl) && !zkUrl.contains(".")) {
        val hbaseClusterMap = PropUtils.sliceKeys("fire.hbase.cluster.map.")
        if (noEmpty(hbaseClusterMap)) {
          val zkAddress = hbaseClusterMap.getOrElse(zkUrl, "")
          if (noEmpty(zkAddress) && zkAddress.contains(":2181")) {
            zkUrl = zkAddress.replaceAll(":2181", "")
          }
        }
      }
      this.toHudiConf(("hoodie.index.hbase.zkquorum", zkUrl), keyNum)
      this.toHudiConf(("hoodie.index.hbase.zknode.path", hbaseZkNodePath), keyNum)
      this.toHudiConf(("hoodie.index.hbase.table", hbaseTable), keyNum)
      this.toHudiConf(("hoodie.index.hbase.put.batch.size.autocompute", hbasePutBatchSizeAutoCompute.toString), keyNum)
      this.toHudiConf(("hoodie.index.hbase.put.batch.size", hbasePutBatchSize.toString), keyNum)
      this.toHudiConf(("hoodie.index.hbase.get.batch.size", hbaseGetBatchSize.toString), keyNum)
      this.toHudiConf(("hoodie.hbase.index.update.partition.path", hbaseUpdatePartitionPath.toString), keyNum)
      this.toHudiConf(("hoodie.index.hbase.rollback.sync", hbaseRollbackSync.toString), keyNum)
      this.toHudiConf(("hoodie.index.hbase.qps.fraction", hbaseQpsFraction.toString), keyNum)
      this.toHudiConf(("hoodie.index.hbase.max.qps.per.region.server", hbaseMaxQpsPerRegionServer.toString), keyNum)
      this.toHudiConf(("hoodie.index.hbase.qps.allocator.class", hbaseQpsAllocatorClass), keyNum)
      this.toHudiConf(("hoodie.index.hbase.user", hbaseIndexUser), keyNum)
    }
  }


  /**
   * hudi clean相关配置
   *
   * @param cleanAsync
   * 是否开启异步clean
   * @param cleanerPolicy
   * clean的策略
   * @param cleanerCommitsRetained
   * clean保留的最大版本数
   */
  @Internal
  private[this] def hudiCleanConf(cleanAsync: Boolean, cleanerPolicy: String, cleanerCommitsRetained: Int, keyNum: Int): Unit = {
    this.toHudiConf(("hoodie.clean.async", cleanAsync.toString), keyNum)
    this.toHudiConf(("hoodie.cleaner.policy", cleanerPolicy), keyNum)
    if (cleanerCommitsRetained > 0) {
      this.toHudiConf(("hoodie.cleaner.commits.retained", cleanerCommitsRetained.toString), keyNum)
    }
  }

  /**
   * 用于配置hudi任务的compaction参数
   */
  @Internal
  private[this] def hudiClusteringConf(clusterCommits: Int, clusterSchedule: Boolean, clustringColumns: String, clusteringPartitions: Int, keyNum: Int): Unit = {
    if (clusterCommits > 0) {
      this.toHudiConf(("hoodie.clustering.inline.max.commits", clusterCommits.toString), keyNum)
      this.toHudiConf(("hoodie.clustering.plan.strategy.daybased.lookback.partitions", clusteringPartitions.toString), keyNum)
      if (noEmpty(clustringColumns)) {
        this.toHudiConf(("hoodie.clustering.plan.strategy.sort.columns", clustringColumns), keyNum)
      }

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
        // 当开启同步compaction时，需关闭推测机制，推测机制的执行会导致compaction不稳定
        PropUtils.setProperty("spark.speculation", "false")
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
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc6
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc6(jdbc: Jdbc6): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._6)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc7
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc7(jdbc: Jdbc7): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._7)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc8
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc8(jdbc: Jdbc8): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._8)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc9
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc9(jdbc: Jdbc9): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._9)
  }


  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc10
   */
  @Internal
  def mapJdbc10(jdbc: Jdbc10): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._10)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   *
   * @param Jdbc11
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc11(jdbc: Jdbc11): Unit = {
    this.mapJdbcConf(jdbc.url(), jdbc.driver(), jdbc.username(), jdbc.password(), jdbc.isolationLevel(),
      jdbc.maxPoolSize(), jdbc.minPoolSize(), jdbc.initialPoolSize(), jdbc.acquireIncrement(), jdbc.maxIdleTime(),
      jdbc.batchSize(), jdbc.flushInterval(), jdbc.maxRetries(), jdbc.storageLevel(), jdbc.queryPartitions(), jdbc.logSqlLength(), jdbc.connectionTimeout, jdbc.config(), KeyNum._11)
  }


  /**
   * 用于映射Kafka相关配置信息
   */
  @Internal
  private def mapKafkaConf(brokers: String, topics: String, groupId: String, startingOffset: String,
                           endingOffsets: String, autoCommit: Boolean, sessionTimeout: Long, requestTimeout: Long,
                           pollInterval: Long, startFromTimestamp: Long, startFromGroupOffsets: Boolean,
                           forceOverwriteStateOffset: Boolean, forceAutoCommit: Boolean, forceAutoCommitInterval: Long,
                           sinkBatch: Int, sinkFlushInterval: Long,
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
    this.put(KAFKA_SINK_BATCH, sinkBatch, keyNum)
    this.put(KAFKA_SINK_FLUSH_INTERVAL, sinkFlushInterval, keyNum)
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
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._1
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
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._2
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
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._3
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
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._4
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
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._5
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka6
   * Kafka注解实例
   */
  @Internal
  def mapKafka6(kafka: Kafka6): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._6
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka7
   * Kafka注解实例
   */
  @Internal
  def mapKafka7(kafka: Kafka7): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._7
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka8
   * Kafka注解实例
   */
  @Internal
  def mapKafka8(kafka: Kafka8): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._8
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka9
   * Kafka注解实例
   */
  @Internal
  def mapKafka9(kafka: Kafka9): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._9
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka10
   */
  @Internal
  def mapKafka10(kafka: Kafka10): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._10
    )
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   *
   * @param Kafka11
   * Kafka注解实例
   */
  @Internal
  def mapKafka11(kafka: Kafka11): Unit = {
    this.mapKafkaConf(kafka.brokers(), kafka.topics(), kafka.groupId(), kafka.startingOffset(),
      kafka.endingOffsets(), kafka.autoCommit(), kafka.sessionTimeout(), kafka.requestTimeout(), kafka.pollInterval(),
      kafka.startFromTimestamp(), kafka.startFromGroupOffsets(), kafka.forceOverwriteStateOffset(),
      kafka.forceAutoCommit(), kafka.forceAutoCommitInterval(), kafka.sinkBatch(), kafka.sinkFlushInterval(), kafka.config(), KeyNum._11
    )
  }

  /**
   * 将@RocketMQ中配置的信息映射为键值对形式
   *
   * @param RocketMQ
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQConf(brokers: String, topics: String, groupId: String, consumerTag: String, startingOffset: String, autoCommit: Boolean, sinkBatch: Int, sinkFlushInterval: Long, config: Array[String], keyNum: Int = KeyNum._1): Unit = {
    this.put(ROCKET_BROKERS_NAME, brokers, keyNum)
    this.put(ROCKET_TOPICS, topics, keyNum)
    this.put(ROCKET_GROUP_ID, groupId, keyNum)
    this.put(ROCKET_CONSUMER_TAG, consumerTag, keyNum)
    this.put(ROCKET_STARTING_OFFSET, startingOffset, keyNum)
    this.put(ROCKET_ENABLE_AUTO_COMMIT, autoCommit, keyNum)
    this.put(ROCKET_SINK_BATCH, sinkBatch, keyNum)
    this.put(ROCKET_SINK_FLUSH_INTERVAL, sinkFlushInterval, keyNum)
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
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._1)
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
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._2)
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
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._3)
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
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._4)
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
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._5)
  }


  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ6
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ6(rocketmq: RocketMQ6): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._6)
  }

  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ7
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ7(rocketmq: RocketMQ7): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._7)
  }

  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ8
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ8(rocketmq: RocketMQ8): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._8)
  }

  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ9
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ9(rocketmq: RocketMQ9): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._9)
  }

  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ10
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ10(rocketmq: RocketMQ10): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._10)
  }


  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   *
   * @param RocketMQ11
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ11(rocketmq: RocketMQ11): Unit = {
    this.mapRocketMQConf(rocketmq.brokers(), rocketmq.topics, rocketmq.groupId, rocketmq.tag,
      rocketmq.startingOffset, rocketmq.autoCommit, rocketmq.sinkBatch(), rocketmq.sinkFlushInterval(), rocketmq.config, KeyNum._11)
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
    if (noEmpty(hive.defaultDB())) this.put(FireHiveConf.DEFAULT_DATABASE_NAME, hive.defaultDB())

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
    classOf[HBase6], classOf[HBase7], classOf[HBase8], classOf[HBase9], classOf[HBase10], classOf[HBase11],
    classOf[Jdbc], classOf[Jdbc2], classOf[Jdbc3], classOf[Jdbc4], classOf[Jdbc5], classOf[Jdbc6], classOf[Jdbc7], classOf[Jdbc8], classOf[Jdbc9],
    classOf[Jdbc10],
    classOf[Jdbc11], classOf[Kafka],
    classOf[Kafka2], classOf[Kafka3], classOf[Kafka4], classOf[Kafka5], classOf[Kafka6],
    classOf[Kafka7], classOf[Kafka8], classOf[Kafka9], classOf[Kafka10], classOf[Kafka11],
    classOf[RocketMQ], classOf[RocketMQ2],
    classOf[RocketMQ3], classOf[RocketMQ4], classOf[RocketMQ5], classOf[RocketMQ6], classOf[RocketMQ7], classOf[RocketMQ8], classOf[RocketMQ9], classOf[RocketMQ10], classOf[RocketMQ11],
    classOf[Hudi], classOf[Hudi2], classOf[Hudi3],
    classOf[Hudi4], classOf[Hudi5], classOf[Hudi6], classOf[Hudi7], classOf[Hudi8], classOf[Hudi9], classOf[Hudi10], classOf[Hudi11]
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
    }(this.logger, "业务逻辑代码执行完成", "业务逻辑代码执行失败", isThrow = true)
  }

  /**
   * 用于调用指定的被注解标记的声明周期方法
   */
  protected[fire] def lifeCycleAnno(baseFire: BaseFire, annoClass: Class[_ <: Annotation]): Unit = {
    tryWithLog {
      ReflectionUtils.invokeAnnoMethod(baseFire, annoClass)
    }(this.logger, "生命周期方法调用成功", "生命周期方法调用失败", isThrow = true)
  }
}
