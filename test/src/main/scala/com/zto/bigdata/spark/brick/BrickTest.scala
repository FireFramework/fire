package com.zto.bigdata.spark.brick

import java.util.concurrent.{ExecutorService, Executors}

import com.zto.bigdata.spark.bean.{DcDispEvent, DcDispItem}
import com.zto.bigdata.spark.common.core.BaseSparkStreaming
import com.zto.bigdata.spark.common.db.JdbcOper
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.KryoUtils
import org.apache.commons.beanutils.BeanUtils
import org.apache.rocketmq.common.message.MessageExt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable.ListBuffer

/**
  * Spark实时消费RocketMQ例子
  *
  * @author ChengLong 2019-6-5 11:36:05
  */
object BrickTest extends BaseSparkStreaming {
  // 线程池大小为2，保证顺序执行
  val brickThreadPool: ExecutorService = Executors.newFixedThreadPool(2)
  // yellowbrick中的item表名
  val itemTableName = "tmp.tmp_dw_cj_dc_disp_item"
  // yellowbrick中的event表名
  val eventTableName = "tmp.tmp_dw_cj_dc_disp_event"
  // 当前批次消息根据id去重过的item临时表
  val itemTmpTableName = "tmp_item"
  // 当前批次消息根据id去重过的event临时表
  val eventTmpTableName = "tmp_event"

  /**
    * item处理过程
    * @param itemStream
    * @param rdd
    */
  private def doItem(itemStream: InputDStream[MessageExt], rdd: RDD[MessageExt]): Unit = {
    val itemRDD = rdd.mapPartitions(it => {
      val bean = ListBuffer[DcDispItem]()
      try {
        it.foreach(msg => {
          val map = KryoUtils.deserializationMap(msg.getBody)
          val item = new DcDispItem()
          BeanUtils.copyProperties(item, map)
          bean += item
        })
      }
      bean.iterator
    })

    this.spark.createDataFrame(itemRDD, classOf[DcDispItem]).createOrReplaceTempViewCache(this.itemTmpTableName)
    // 同一个id，取version最大的
    val itemDF = this.spark.sql(BrickTestSQL.mergeByVersion(this.itemTmpTableName)).cache
    // 删除已存在的记录
    this.deleteById(itemDF.select("id").coalesce(20), this.itemTableName)
    // 插入数据到数据库中
    itemDF.createOrReplaceTempViewCache("item")
    // this.spark.sql(BrickTestSQL.itemFields("item")).coalesce(20).saveAsJDBCTable(this.itemTableName)
    this.spark.sql(BrickTestSQL.itemFields("item")).coalesce(20).jdbcTableSave(this.itemTableName)

    this.spark.unpersist(itemDF, "item", this.itemTmpTableName)
    rdd.rocketCommitOffsets(itemStream)
  }

  /**
    * event处理逻辑
    * @param eventStream
    * @param rdd
    */
  private def doEvent(eventStream: InputDStream[MessageExt], rdd: RDD[MessageExt]): Unit = {
    val eventRDD = rdd.mapPartitions(it => {
      val bean = ListBuffer[DcDispEvent]()
      it.foreach(msg => {
        val map = KryoUtils.deserializationMap(msg.getBody)
        val event = new DcDispEvent()
        BeanUtils.copyProperties(event, map)
        bean += event
      })
      bean.iterator
    })

    this.spark.createDataFrame(eventRDD, classOf[DcDispEvent]).createOrReplaceTempViewCache(this.eventTmpTableName)
    // 同一个id，取version最大的
    val eventDF = this.spark.sql(BrickTestSQL.mergeByVersion(this.eventTmpTableName)).cache()
    // 删除已存在的记录
    this.deleteById(eventDF.select("id").coalesce(20), this.itemTableName)
    eventDF.createOrReplaceTempViewCache("event")
    // this.spark.sql(BrickTestSQL.eventFields("event")).coalesce(20).saveAsJDBCTable(this.eventTableName)
    this.spark.sql(BrickTestSQL.eventFields("event")).coalesce(20).jdbcTableSave(this.eventTableName)

    this.spark.uncache(eventDF, "event", this.eventTmpTableName)
    rdd.rocketCommitOffsets(eventStream)
  }


  /**
    * 根据id删除指定表中的记录
    *
    * @param idDataFrame
    * id集合
    */
  def deleteById(idDataFrame: DataFrame, tableName: String): Unit = {
    idDataFrame.rdd.foreachPartition(it => {
      val sqlStart = s"delete from ${tableName} where id in ("
      val sql = new StringBuilder(sqlStart)
      it.foreach(row => {
        sql.append(row.getLong(0) + ",")
      })
      val finalSql = sql.substring(0, sql.length() - 1) + ")"
      JdbcOper.executeUpdate(finalSql, null)
    })
  }

  /**
    * bill_item数据处理
    */
  def processItem: Unit = {
    // 以pull方式消费RocketMQ中的数据
    val itemStream = this.ssc.createRocketPullStream()
    itemStream.foreachRDD(rdd => {
      // 第一个参数为自己定义的一个函数，会在子线程中提交job执行
      // threadPool参数为自己定义的线程池，用于限制最多可以并行多少个job
      // 如果不指定，则默认使用内置的大小为20的线程池，但这通常不是想要的
      this.runAsThread(this.doItem(itemStream, rdd), threadPool = this.brickThreadPool)
      // this.doItem(itemStream, rdd)
    })
  }

  /**
    * bill_event数据处理
    */
  def processEvent: Unit = {
    val eventStream = this.ssc.createRocketPullStream2()
    eventStream.foreachRDD(rdd => {
      // this.runAsThread(this.doEvent(eventStream, rdd), threadPool = this.brickThreadPool)
      this.doEvent(eventStream, rdd)
    })
  }

  /**
    * 处理流程
    */
  override def process: Unit = {
    this.processItem
    this.processEvent
    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
    this.spark.stop()
  }

}
