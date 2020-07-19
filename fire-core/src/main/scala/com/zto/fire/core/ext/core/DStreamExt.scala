package com.zto.fire.core.ext.core

import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.common.bean.ogg.OGGBean
import com.zto.fire.common.util.FireUtils
import com.zto.fire.core.ext.module.HBaseContextExt
import com.zto.fire.core.util.SingletonFactory
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

import scala.collection.mutable.ListBuffer
import scala.reflect._

/**
 * DStream扩展
 *
 * @param stream
 * stream对象
 * @author ChengLong 2019-5-18 11:06:56
 */
class DStreamExt[T: ClassTag](stream: DStream[T]) {

  // 获取单例的HBaseContext对象
  private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(stream.context.sparkContext)

  /**
   * DStrea数据实时写入
   *
   * @param tableName
   * HBase表名
   */
  def hbaseBulkPutStream[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    this.hbaseContext.bulkPutStream(tableName, stream.asInstanceOf[DStream[T]], insertEmpty, multiVersion)
  }

  /**
   * 清空RDD的缓存
   */
  def uncache: Unit = {
    stream.persist(StorageLevel.NONE)
  }

  /**
   * 维护kafka的offset
   */
  def kafkaCommitOffsets[T <: ConsumerRecord[String, String]]: Unit = {
    stream.asInstanceOf[DStream[T]].foreachRDD { rdd =>
      try {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  /**
   * 维护RocketMQ的offset
   */
  def rocketCommitOffsets[T <: MessageExt]: Unit = {
    stream.asInstanceOf[DStream[T]].foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        try {
          val offsetRanges = rdd.asInstanceOf[org.apache.rocketmq.spark.HasOffsetRanges].offsetRanges
          stream.asInstanceOf[org.apache.rocketmq.spark.CanCommitOffsets].commitAsync(offsetRanges)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  /**
   * 解析ogg中的json数据为指定的JavaBean类型
   * 支持消息格式为json和jsonarray
   *
   * @param clazz
   * 目标类型
   * @param paseAfter
   * 是否解析after数据
   * @param paseBefore
   * 是否解析before数据
   * @return
   * 对应类型的DStream
   */
  def mapOgg[E: ClassTag](clazz: Class[E], paseAfter: Boolean = true, paseBefore: Boolean = true): DStream[OGGBean[E]] = {
    if (!this.stream.isInstanceOf[DStream[ConsumerRecord[String, String]]]) throw new IllegalArgumentException("ogg消息解析失败：DStream必须为String类型")
    this.stream.mapPartitions(it => {
      val list = ListBuffer[OGGBean[E]]()
      it.foreach(msg => {
        if (msg != null) {
          val record = msg.asInstanceOf[ConsumerRecord[String, String]].value()
          val json = StringUtils.trim(record)
          if (StringUtils.isNotBlank(json)) {
            if (json.startsWith("[") && json.endsWith("]")) {
              // json array
              val oggList = FireUtils.oggJsonArrayParse(json, clazz, paseAfter, paseBefore)
              if (oggList != null && oggList.size > 0) list ++= oggList
            } else if (json.startsWith("{") && json.endsWith("}")) {
              // json
              val ogg = FireUtils.oggJsonParse(json, clazz, paseAfter, paseBefore)
              if (ogg != null) list += ogg
            } else {
              throw new IllegalArgumentException("ogg消息解析失败：json格式不合法")
            }
          }
        }
      })
      list.iterator
    })
  }
}