package com.zto.fire.common.util

import java.util
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

/**
 * Kafka工具类
 *
 * @author ChengLong 2020-4-17 09:50:50
 */
object KafkaUtils {
  // 当前kafka监控的groupId
  private lazy val kafkaMonitor = "bigdata_kafka_monitor"
  private lazy val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val kafkaCluster = new java.util.HashMap[String, String]
  this.init

  /**
   * 初始化kafka连接信息
   */
  private def init: Unit = {
    // 大数据kafka集群
    kafkaCluster.put("bigdata", "192.168.25.80:9092,192.168.25.81:9092,192.168.25.82:9092,192.168.25.129:9092,192.168.25.130:9092,192.168.25.131:9092")
    // zms kafka集群
    kafkaCluster.put("zms", "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092,192.168.1.173:9092,192.168.5.29:9092,192.168.5.30:9092")
    // 新的kafka集群
    kafkaCluster.put("zmsNew", "192.168.73.31:9092,192.168.73.32:9092,192.168.73.33:9092,192.168.73.34:9092,192.168.73.35:9092,192.168.73.36:9092")
    // 测试环境集群
    kafkaCluster.put("test", "10.9.45.97:9092,10.9.15.38:9092,10.9.36.49:9092,10.9.36.50:9092")
    // 新增kafka集群(宋昉)
    kafkaCluster.put("kafka-ai", "10.9.30.13:9092,10.9.30.14:9092,10.9.30.15:9092,10.9.30.16:9092,10.9.30.17:9092")
  }

  /**
   * 根据kafka集群名称获取broker地址
   *
   * @param clusterName 集群名称
   * @return broker地址
   */
  def getBorkers(clusterName: String): String = {
    if (StringUtils.isNotBlank(clusterName)) return kafkaCluster.get(clusterName)
    kafkaCluster.get("zms")
  }

  /*
  /**
   * 获取指定topic每一个partition的最新offset
   *
   * @param host  broker地址
   * @param topic topic名称
   * @return partition offset
   */
  def getLogEndOffset(host: String, topic: String): util.Map[TopicPartition, Long] = {
    val endOffsets = new ConcurrentHashMap[TopicPartition, Long]
    val consumer = createNewConsumer(host, kafkaMonitor)
    val partitionInfoList = consumer.partitionsFor(topic)
    val topicPartitions: java.util.List[TopicPartition] = partitionInfoList.stream.map((pi: PartitionInfo) => new TopicPartition(topic, pi.partition)).collect(Collectors.toList)
    consumer.assign(topicPartitions)
    consumer.seekToEnd(topicPartitions)
    topicPartitions.forEach((topicPartition: TopicPartition) => endOffsets.put(topicPartition, consumer.position(topicPartition)))
    consumer.close()
    endOffsets
  }*/

  /**
   * 创建新的kafka consumer
   *
   * @param host    kafka broker地址
   * @param groupId 对应的groupId
   * @return KafkaConsumer
   */
  def createNewConsumer(host: String, groupId: String): KafkaConsumer[String, String] = {
    val properties = new Properties
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    properties.put("auto.offset.reset", "earliest")
    new KafkaConsumer[String, String](properties)
  }

  /**
   * 获取大于指定时间戳的一条消息
   *
   * @param host      broker地址
   * @param topic     topic信息
   * @param timestamp 消息时间戳
   * @return 一条消息记录
   */
  def getMsg(host: String, topic: String, timestamp: java.lang.Long): String = {
    var kafkaConsumer: KafkaConsumer[String, String] = null
    var msg = ""
    try {
      kafkaConsumer = createNewConsumer(host, kafkaMonitor)
      // 如果指定了时间戳，则取大于该时间戳的消息
      if (timestamp != null) { // 获取topic的partition信息
        val partitionInfos = kafkaConsumer.partitionsFor(topic)
        val topicPartitions = new util.ArrayList[TopicPartition]
        val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]
        import scala.collection.JavaConversions._
        for (partitionInfo <- partitionInfos) {
          topicPartitions.add(new TopicPartition(partitionInfo.topic, partitionInfo.partition))
          timestampsToSearch.put(new TopicPartition(partitionInfo.topic, partitionInfo.partition), timestamp)
        }
        // 手动指定各分区offset
        kafkaConsumer.assign(topicPartitions)
        // 获取每个partition指定时间戳的偏移量
        val map = kafkaConsumer.offsetsForTimes(timestampsToSearch)
        System.out.println("根据时间戳获取偏移量：map.size=" + map.size)
        var offsetTimestamp: OffsetAndTimestamp = null
        System.out.println("开始设置各分区初始偏移量...")
        import scala.collection.JavaConversions._
        for (entry <- map.entrySet) { // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
          offsetTimestamp = entry.getValue
          if (offsetTimestamp != null) { // 设置读取消息的偏移量
            val offset: java.lang.Long = offsetTimestamp.offset
            kafkaConsumer.seek(entry.getKey, offset)
            System.out.println("seek: id=" + entry.getKey.partition + " offset=" + offset)
          }
        }
      }
      else { // 如果未指定时间戳，则直接获取消息
        kafkaConsumer.subscribe(util.Arrays.asList(topic))
      }
      // 消费消息
      val records = kafkaConsumer.poll(10000)
      import scala.collection.JavaConversions._
      for (record <- records if StringUtils.isBlank(msg)) {
        if (timestamp == null) {
          msg = record.value
        }
        else { // 如果指定时间戳，则取大于指定时间戳的消息
          if (record.timestamp >= timestamp) {
            msg = record.value
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.error("获取消息失败", e)
    } finally if (kafkaConsumer != null) kafkaConsumer.close()
    msg
  }

  /**
   * kafka配置信息
   *
   * @param groupId
   * 消费组
   * @param offset
   * smallest、largest
   * @return
   * kafka相关配置
   */
  def kafkaParams(groupId: String = null, kafkaBrokers: String = null, offset: String = null, autoCommit: Boolean = false, keyNum: Int = 1): Map[String, Object] = {
    ValueUtils.requireNonNull(groupId, s"kafka groupId不能为空，请在配置文件中指定：spark.kafka.group.id$keyNum")

    val finalKafkaBrokers = if (StringUtils.isBlank(kafkaBrokers)) GlobalConstants.KafkaConf.kafkaBrokers(keyNum) else kafkaBrokers
    ValueUtils.requireNonNull(finalKafkaBrokers, s"kafka broker地址不能为空，可在配置文件中指定[ spark.kafka.brokers.name$keyNum ]")

    val finalOffset = if (StringUtils.isBlank(offset)) GlobalConstants.KafkaConf.kafkaStartingOffset(keyNum) else offset
    val finalAutoCommit = if (GlobalConstants.KafkaConf.kafkaEnableAutoCommit(keyNum) != null) GlobalConstants.KafkaConf.kafkaEnableAutoCommit(keyNum) else autoCommit

    val consumerMap = collection.mutable.Map[String, Object](
      "bootstrap.servers" -> finalKafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> finalOffset,
      "enable.auto.commit" -> (finalAutoCommit: java.lang.Boolean),
      "session.timeout.ms" -> GlobalConstants.KafkaConf.kafkaSessionTimeOut(keyNum),
      "request.timeout.ms" -> GlobalConstants.KafkaConf.kafkaRequestTimeOut(keyNum),
      "max.poll.interval.ms" -> GlobalConstants.KafkaConf.kafkaPollInterval(keyNum)
    )

    // 心跳间隔时间
    val heartbeatInterval = GlobalConstants.KafkaConf.kafkaHeartbeatInterval(keyNum)
    if (heartbeatInterval > 0) {
      consumerMap += ("heartbeat.interval.ms" -> heartbeatInterval)
    }
    // 消费者组最大的session超时时间
    val groupMaxSessionTimeOut = GlobalConstants.KafkaConf.kafkaGroupMaxSessionTimeOut(keyNum)
    if (groupMaxSessionTimeOut > 0) {
      consumerMap += ("group.max.session.timeout.ms" -> groupMaxSessionTimeOut)
    }
    // 消费者组最小的session超时时间
    val groupMinSessionTimeOut = GlobalConstants.KafkaConf.kafkaGroupMinSessionTimeOut(keyNum)
    if (groupMinSessionTimeOut > 0) {
      consumerMap += ("group.min.session.timeout.ms" -> groupMinSessionTimeOut)
    }
    // 一次调用pool返回的最大记录数
    val maxPollRecords = GlobalConstants.KafkaConf.kafkaMaxPollRecords(keyNum)
    if (maxPollRecords > 0) {
      consumerMap += ("max.poll.records" -> maxPollRecords)
    }
    // 每个分区返回的最大数据量
    val maxPartitionFetchBytes = GlobalConstants.KafkaConf.kafkaMaxPartitionFetchBytes(keyNum)
    if (maxPartitionFetchBytes > 0) {
      consumerMap += ("max.partition.fetch.bytes" -> maxPartitionFetchBytes)
    }

    consumerMap.toMap
  }

}
