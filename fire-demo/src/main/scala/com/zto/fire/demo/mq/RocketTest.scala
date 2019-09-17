package com.zto.fire.demo.mq

import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.common.util.KryoUtils
import com.zto.fire.core.BaseSparkStreaming

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
    val eventStream = this.ssc.createRocketPullStream(keyNum = 2)

    val msgDStream = itemStream.mapPartitions(it => {
      val list = ListBuffer[(String, String)]()
      it.foreach(msg => {
        // 反序列化
        val map = KryoUtils.deserializationMap(msg.getBody)
        val scalaMap = JavaConversions.mapAsScalaMap(map)
        list ++= scalaMap.toList
      })

      this.acc.addMultiCounter("item", list.size)
      list.iterator
    })
    println("=============item===========")
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
      this.acc.addMultiCounter("event", list.size)
      list.iterator
    })
    println("=============event===========")
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
