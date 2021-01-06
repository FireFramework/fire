package com.zto.fire.spark.ext.core

import com.zto.fire._
import com.zto.fire.jdbc.JdbcConnectorBridge
import com.zto.fire.spark.ext.provider._
import com.zto.fire.spark.util.SparkSingletonFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.reflect.ClassTag

/**
 * SparkContext扩展
 *
 * @param spark
 * sparkSession对象
 * @author ChengLong 2019-5-18 10:51:19
 */
private[fire] class SparkSessionExt(spark: SparkSession) extends JdbcConnectorBridge with JdbcSparkProvider
  with HBaseBulkProvider with SqlProvider with HBaseConnectorProvider with HBaseHadoopProvider with KafkaSparkProvider {
  private[fire] lazy val ssc = SparkSingletonFactory.getStreamingContext
  private[this] lazy val appName = ssc.sparkContext.appName

  /**
   * 根据给定的集合，创建rdd
   *
   * @param seq
   * seq
   * @param numSlices
   * 分区数
   * @return
   * RDD
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
    this.sc.parallelize(seq, numSlices)
  }

  /**
   * 根据给定的集合，创建rdd
   *
   * @param seq
   * seq
   * @param numSlices
   * 分区数
   * @return
   * RDD
   */
  def createRDD[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
    this.parallelize[T](seq, numSlices)
  }

  /**
   * 构建Kafka DStream流
   *
   * @param kafkaParams
   * kafka参数
   * @param topics
   * topic列表
   * @return
   * DStream
   */
  def createKafkaDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, groupId: String = null, keyNum: Int = 1): DStream[ConsumerRecord[String, String]] = {
    this.ssc.createDirectStream(kafkaParams, topics, groupId, keyNum)
  }

  /**
   * 构建RocketMQ拉取消息的DStream流
   *
   * @param rocketParam
   * rocketMQ相关消费参数
   * @param groupId
   * groupId
   * @param topics
   * topic列表
   * @param consumerStrategy
   * 从何处开始消费
   * @return
   * rocketMQ DStream
   */
  def createRocketMqPullStream(rocketParam: Map[String, String] = null,
                               groupId: String = this.appName,
                               topics: String = null,
                               tag: String = null,
                               consumerStrategy: ConsumerStrategy = ConsumerStrategy.lastest,
                               locationStrategy: LocationStrategy = LocationStrategy.PreferConsistent,
                               keyNum: Int = 1): InputDStream[MessageExt] = {
    this.ssc.createRocketPullStream(rocketParam, groupId, topics, tag, consumerStrategy, locationStrategy, keyNum)
  }

  /**
   * 启动StreamingContext
   */
  def start(): Unit = {
    if (this.ssc != null) {
      this.ssc.start()
    }
  }
}