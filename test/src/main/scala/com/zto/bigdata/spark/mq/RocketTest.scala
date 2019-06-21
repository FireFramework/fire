package com.zto.bigdata.spark.mq

import com.zto.bigdata.spark.common.core.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.KryoUtils

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

/**
  * 盘古数据实时同步
  */
object RocketTest extends BaseSparkStreaming {

  override def process: Unit = {
    // 以pull方式消费RocketMQ中的数据
    val itemStream = this.ssc.createRocketPullStream()
    // 多个不同的topic，对应配置文件后缀为2，如：spark.rocket.brokers.name2   spark.rocket.topics2
    val eventStream = this.ssc.createRocketPullStream2()

    val msgDStream = itemStream.mapPartitions(it => {
      val list = ListBuffer[(String, String)]()
      it.foreach(msg => {
        // 反序列化
        val map = KryoUtils.deserializationMap(msg.getBody)
        val scalaMap = JavaConversions.mapAsScalaMap(map)
        list ++= scalaMap.toList
      })
      list.iterator
    })
    msgDStream.print(1)

    // 维护rocket offset
    itemStream.rocketCommitOffsets

    val eventMsgDStream = eventStream.mapPartitions(it => {
      val list = ListBuffer[(String, String)]()
      it.foreach(msg => {
        // 反序列化
        val map = KryoUtils.deserializationMap(msg.getBody)
        val scalaMap = JavaConversions.mapAsScalaMap(map)
        list ++= scalaMap.toList
      })
      list.iterator
    })
    eventMsgDStream.print(1)

    // 维护rocket offset
    eventStream.rocketCommitOffsets

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(30, false)
    this.spark.stop()
  }
}
