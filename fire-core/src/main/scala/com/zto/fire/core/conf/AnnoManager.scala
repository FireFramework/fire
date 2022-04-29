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

import com.zto.fire.predef._
import com.google.common.collect.Sets
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.common.util.{ReflectionUtils}

import scala.collection.mutable.HashMap
import com.zto.fire.core.anno._
import org.apache.commons.lang3.StringUtils


/**
 * 注解管理器：用于将主键中的配置信息映射为键值对信息
 * 注：解析指定的配置注解需要满足以下两个条件：
 *  1. 在registerAnnoSet中注册新的注解
 *  2. 开发对应的map方法，如：mapHive解析@Hive、mapKafka解析@kafka注解
 *
 * @author ChengLong 2022-04-26 11:19:00
 * @since 2.2.2
 */
@Internal
private[fire] trait AnnoManager {
  protected[fire] lazy val props = new HashMap[String, String]()
  // 用于存放注册了的主键，只解析这些主键中的信息
  protected[fire] lazy val registerAnnoSet = Sets.newHashSet[Class[_]](
    classOf[Hive], classOf[HBase], classOf[HBase2], classOf[HBase3],
    classOf[Jdbc], classOf[Jdbc2], classOf[Jdbc3],
    classOf[Kafka], classOf[Kafka2], classOf[Kafka3],
    classOf[RocketMQ], classOf[RocketMQ2], classOf[RocketMQ3],
  )

  this.register

  /**
   * 用于注册需要映射配置信息的自定义主键
   */
  @Internal
  protected[fire] def register: Unit

  /**
   * 用于映射Hbase相关配置信息
   * @param value
   * 对应注解中的value
   * @param config
   * 对应注解中的config
   */
  @Internal
  private def mapHBaseConf(value: String, family: String, batchSize: Int,
                           scanPartitions: Int, storageLevel: String, maxRetries: Int, durability: String,
                           tableMetaCache: Boolean, config: Array[String], keyNum: Int = 1): Unit = {

    this.put("hbase.cluster", value, keyNum)
    this.put("hbase.column.family", family, keyNum)
    this.put("fire.hbase.batch.size", batchSize, keyNum)
    this.put("fire.hbase.scan.partitions", scanPartitions, keyNum)
    this.put("fire.hbase.storage.level", storageLevel, keyNum)
    this.put("hbase.max.retry", maxRetries, keyNum)
    this.put("hbase.durability", durability, keyNum)
    this.put("fire.hbase.table.exists.cache.enable", tableMetaCache, keyNum)

    if (noEmpty(config)) {
      config.foreach(conf => {
        val kv = conf.split("=")
        if (kv != null && kv.length == 2) {
          this.put(s"fire.hbase.conf.${kv(0).trim}", kv(1).trim, keyNum)
        }
      })
    }
  }

  /**
   * 将@HBase中配置的信息映射为键值对形式
   * @param HBase
   * HBase注解实例
   */
  @Internal
  def mapHBase(hbase: HBase): Unit = this.mapHBaseConf(hbase.value(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), keyNum = 1)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   * @param HBase2
   * HBase注解实例
   */
  @Internal
  def mapHBase2(hbase: HBase2): Unit = this.mapHBaseConf(hbase.value(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), keyNum = 2)

  /**
   * 将@HBase中配置的信息映射为键值对形式
   * @param HBase3
   * HBase注解实例
   */
  @Internal
  def mapHBase3(hbase: HBase3): Unit = this.mapHBaseConf(hbase.value(), hbase.family(), hbase.batchSize(), hbase.scanPartitions(), hbase.storageLevel(), hbase.maxRetries(), hbase.durability(), hbase.tableMetaCache(), hbase.config(), keyNum = 3)

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   * @param Jdbc
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc(jdbc: Jdbc): Unit = {
    println("调用：mapJdbc -> value=" + jdbc.url() + " " + jdbc.driver().getDriver)
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   * @param Jdbc
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc2(jdbc: Jdbc2): Unit = {
    println("调用：mapJdbc2 -> value=" + jdbc.url())
  }

  /**
   * 将@Jdbc中配置的信息映射为键值对形式
   * @param Jdbc3
   * Jdbc注解实例
   */
  @Internal
  def mapJdbc3(jdbc: Jdbc3): Unit = {
    println("调用：mapJdbc3 -> value=" + jdbc.url())
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   * @param Kafka
   * Kafka注解实例
   */
  @Internal
  def mapKafka(kafka: Kafka): Unit = {
    println("调用：mapKafka -> value=" + kafka.brokers())
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   * @param Kafka2
   * Kafka注解实例
   */
  @Internal
  def mapKafka2(kafka: Kafka2): Unit = {
    println("调用：mapKafka2 -> value=" + kafka.brokers())
  }

  /**
   * 将@Kafka中配置的信息映射为键值对形式
   * @param Kafka3
   * Kafka注解实例
   */
  @Internal
  def mapKafka3(kafka: Kafka3): Unit = {
    println("调用：mapKafka3 -> value=" + kafka.brokers())
  }

  /**
   * 将@RocketMQ中配置的信息映射为键值对形式
   * @param RocketMQ
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ(rocketmq: RocketMQ): Unit = {
    println("调用：mapRocketMQ -> value=" + rocketmq.brokers())
  }

  /**
   * 将@RocketMQ2中配置的信息映射为键值对形式
   * @param RocketMQ2
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ2(rocketmq: RocketMQ2): Unit = {
    println("调用：mapRocketMQ2 -> value=" + rocketmq.brokers())
  }

  /**
   * 将@RocketMQ3中配置的信息映射为键值对形式
   * @param RocketMQ3
   * RocketMQ注解实例
   */
  @Internal
  def mapRocketMQ3(rocketmq: RocketMQ3): Unit = {
    println("调用：mapRocketMQ3 -> value=" + rocketmq.brokers())
  }

  /**
   * 将@Hive中配置的信息映射为键值对形式
   * @param Hive
   * Hive注解实例
   */
  @Internal
  def mapHive(hive: Hive): Unit = {
    if (noEmpty(hive.value())) this.put(FireHiveConf.HIVE_CLUSTER, hive.value())
    if (noEmpty(hive.catalog())) this.put(FireHiveConf.HIVE_CATALOG_NAME, hive.catalog())
    if (noEmpty(hive.version())) this.put(FireHiveConf.HIVE_VERSION, hive.version())
    if (noEmpty(hive.partition())) this.put(FireHiveConf.DEFAULT_TABLE_PARTITION_NAME, hive.partition())
  }

  /**
   * 将键值对配置信息存放到map中
   * @param key
   * 配置的key
   * @param value
   * 配置的value
   * @param keyNum
   * 配置key的数字结尾标识
   */
  @Internal
  protected def put(key: String, value: Any, keyNum: Int = 1): this.type = {
    if (noEmpty(key, value)) {
      // 将配置中多余的空格去掉
      val fixKey = StringUtils.trim(key)
      val fixValue = StringUtils.trim(value.toString)

      // 如果keyNum>1则将数值添加到key的结尾
      val realKey = if (keyNum > 1) fixKey + keyNum else fixKey
      val isNumeric = StringUtils.isNumeric(fixValue)

      // 约定注解中指定的配置的值如果为-1，表示不使用该项配置，通常-1表示默认值
      if (!isNumeric || (isNumeric && fixValue.toLong != -1)) {
        this.props.put(realKey, fixValue)
      }
    }
    this
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
    annotations.filter(anno => this.registerAnnoSet.contains(anno.annotationType())).foreach(anno => {
      // 反射调用map+注解名称对应的方法：
      // 比如注解名称为Hive，则调用mapHive方法解析@Hive注解中的配置信息
      val methodName = s"map${anno.annotationType().getSimpleName}"
      if (mapMethods.containsKey(methodName)) {
        mapMethods.get(methodName).invoke(this, anno)
      }
    })

    this.props
  }
}
