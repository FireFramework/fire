package com.zto.fire.examples.spark.hbase

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.BaseSparkStreaming

/**
  * 通过hbase相关api，将数据实时写入到hbase中
  * @author ChengLong 2019-5-26 13:21:59
  */
@Config(
  """
    |# 非必须配置项：默认是大数据的kafka地址，如果连zms，则将bigdata替换为zms
    |spark.kafka.brokers.name           =       bigdata_test
    |# 必须配置项：kafka的topic列表，以逗号分隔
    |spark.kafka.topics                 =       fire
    |# 非必须配置项：默认为appName
    |spark.kafka.group.id               =       fire
    |spark.streaming.batch.duration     =       30
    |spark.hvie.cluster                 =       test
    |
    |# ------------------- < hbase 配置 > ------------------- #
    |# 用于区分不同的hbase集群: batch/streaming/old
    |spark.hbase.cluster                =       test
    |spark.hbase.cluster2               =       test
    |spark.fire.rest.filter.enable      =       false
    |spark.fire.hbase.scan.repartitions =       30
    |spark.fire.hbase.storage.level     =       DISK_ONLY
    |spark.fire.rest.url.hostname       =       true
    |
    |# spark的参数可以直接写在下面，都会被加载，覆盖程序中默认的配置信息
    |spark.speculation                  =       false
    |spark.streaming.concurrentJobs     =       1
    |""")
object HBaseStreamingTest extends BaseSparkStreaming {
  private val tableName8 = "fire_test_8"
  private val tableName9 = "fire_test_9"

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    HBaseConnector.truncateTable(this.tableName8)
    HBaseConnector.truncateTable(this.tableName9, keyNum = 2)

    dstream.repartition(3).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        HBaseConnector.insert(this.tableName8, Student.newStudentList())
        val student = HBaseConnector.get(this.tableName9, classOf[Student], Seq("1", "2"))
        student.foreach(t => logger.error("HBase1 Get结果：" + t))

        HBaseConnector.insert(this.tableName9, Student.newStudentList())
        val student2 = HBaseConnector.get(this.tableName8, classOf[Student], Seq("2", "3"), keyNum = 2)
        student2.foreach(t => logger.error("HBase2 Get结果：" + t))
      })
    })

    this.fire.start()
  }

  override def main(args: Array[String]): Unit = {
    this.init(30, false)
  }
}
