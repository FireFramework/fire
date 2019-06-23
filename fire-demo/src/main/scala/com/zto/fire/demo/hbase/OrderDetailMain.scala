package com.zto.fire.demo.hbase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.common.util.GlobalConstants
import com.zto.fire.core.BaseSparkStreaming

import scala.collection.mutable.ListBuffer

object OrderDetailMain extends BaseSparkStreaming {
  val tableName = "tmp_order2"
  val hbaseTableOrderMain = "order:order_main2"
  val hbaseTableReplicaOrder = "order:order_replica2"
  val brokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
  val topics = Set("")

  def main(args: Array[String]): Unit = {
    this.init(20L, false)

    val ssc = new StreamingContext(this.sc, Seconds(90))
    val kafkaDStream = this.ssc.createDirectStream(this.kafkaParams(this.appName, this.brokers, GlobalConstants.KafkaConf.offsetLargest, false), this.topics)

    kafkaDStream.map(t => t.value()).foreachRDD(rdd => {
      processRdd(rdd)
    })

    ssc.start()
    ssc.awaitTermination()
    this.sc.stop()
    this.threadPool.shutdown()
  }

  def processRdd(rdd: RDD[String]): Unit = {
    this.hiveContext.read.json("").createOrReplaceTempView(this.tableName)
    this.hiveContext.cacheTable(this.tableName)

    this.threadPool.execute(new Runnable() {
      override def run(): Unit = {
        // mainorder表批量插入操作
        val mainOrderDF = hiveContext.sql(HiveQL.saveMainOrder(tableName))
        hbaseContext.hadoopPutDFRow(hbaseTableOrderMain, mainOrderDF, buildMainOrderRowKey)

        // mainorder表批量删除操作
        val mainOrderRowKeyRDD = hiveContext.sql(HiveQL.deleteMainOrder(tableName)).rdd.mapPartitions(it => buildOrderMainRowKey(it))
        hbaseContext.bulkDeleteRDD(hbaseTableOrderMain, mainOrderRowKeyRDD)
      }
    })

    this.threadPool.execute(new Runnable() {
      override def run(): Unit = {
        // replicaOrder表批量插入操作
        val replicaOrderDF = hiveContext.sql(HiveQL.saveReplicaOrder(tableName))
        hbaseContext.hadoopPutDFRow(hbaseTableReplicaOrder, replicaOrderDF, buildReplicaOrderRowKey)
        // replicaOrder表批量删除操作
        val replicaOrderRowKeyRDD = hiveContext.sql(HiveQL.deleteReplicaOrder(tableName)).rdd.map(row => row.getAs[String](0))
        hbaseContext.bulkDeleteRDD(hbaseTableReplicaOrder, replicaOrderRowKeyRDD)
      }
    })

  }

  /**
    * 构建main_order rowkey
    */
  val buildMainOrderRowKey = (row: Row) => {
    val orderCode = row.getAs("order_code").toString
    val billCode = row.getAs("bill_code").toString.reverse
    billCode + "-" + orderCode
  }

  /**
    * 构建replica_order rowkey
    */
  val buildReplicaOrderRowKey = (row: Row) => {
    val orderCode = row.getAs("order_code").toString
    val billCode = row.getAs("bill_code").toString.reverse
    billCode + "-" + orderCode
  }

  /**
    * 构建order:order_main表的rowkey
    * @param it
    * @return
    */
  def buildOrderMainRowKey(it: Iterator[Row]): Iterator[String] = {
    val rowKeyList = ListBuffer[String]()
    it.foreach(row => {
      rowKeyList += (row.getAs[String]("bill_code") + "00000000000000000000").substring(0, 20)
    })
    rowKeyList.iterator
  }
}
