package com.zto.fire.demo.streaming

import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.demo.bean.Senda

/**
  * Streaming类型的实时任务，统一继承自BaseSparkStreaming，并在init中完成初始化
  * streaming任务需在resources目录下创建一个与该类同名的配置文件，如当前类的附属配置文件为resources/ShenzhouSyncStreaming.properties
  * 在ShenzhouSyncStreaming.properties中配置broker地址、groupId名称、topic列表（以逗号分隔）
  *
  * @author ChengLong 2019-5-17 11:02:27
  */
object ShenzhouSyncStreaming extends BaseSparkStreaming {
  val tableName = "spark_hbase_test"

  def main(args: Array[String]): Unit = {
    // 第一个参数表示每个batch的时间，第二个参数用于设定是否开启checkPoint
    // 如果开启checkPoint，则需实现父类的process方法，checkPoint路径为：hdfs://appcluster/user/spark/ckpoint/sparkjob名
    this.init(30L, false)
    // process方法不需要用户手动调用，它会在init方法中自动被调用起来执行
  }


  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    // 默认broker、topic、groupId等信息从该类同名的配置文件中读取，比如当前类名为ShenzhouSyncStreaming，那么默认会从ShenzhouSyncStreaming.properties中读取配置
    // 配置文件中只有topic是必须指定的，groupId默认为appName，broker地址默认为192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092
    // 注：使用该方法需导入：import com.zto.bigdata.spark.common.ext.SparkExt._
    val dstream = this.ssc.createDirectStream()

    dstream.foreachRDD(rdd => {
      // 将json数据解析成Senda对象对应的类型，Senda是一个普通的JavaBean，里面的字段要与
      // kafka json中after的字段一一对应，如果需要before字段，则使用this.parseJson2DataFrame(rdd, classOf[Senda], true)，true表示同时解析before与after的数据，
      // 不写或者写false，则表示只解析after中的数据
      val sendaDF = this.spark.kafkaJson2DF(rdd, classOf[Senda])

      // 通过日志方式打印，日志的级别需在配置文件中spark.log.level=INFO指定
      // this.wrapLogError("日志：" + sendaDF.count())

      // 将DataFrame中的数据以hbase的java api方式写入到指定表中，第二个参数Senda中的字段与hbase表中的字段一一对应
      // Senda类型必须是HBaseBaseBean的子类，并且要在这个JavaBean中实现buildRowKey方法
      sendaDF.hbaseOperPutDF(this.tableName, classOf[Senda])

      // 参数含义：第三个参数表示是否将JavaBean中的空值写入到hbase中，true表示写。batchSize表示每个批次最多写多少条数据
      // multiVersion表示是否多版本写入，当创建的表为多版本表时，该参数需要设置为true
      // sendaDF.hbaseOperPutDF(this.tableName, classOf[Senda], false, 1000, false)
    })

    // 以单独的线程执行count方法，只执行一次
    this.runAsThread(this.count1)

    // 以单独线程方式循环执行loop方法，延迟1分钟，每隔1分钟执行一次
    this.runAsSchedule(this.loop, 1, 1)

    this.ssc.start()
    this.ssc.awaitTermination()
  }

  /**
    * 单独线程执行
    */
  def count1: Unit = {
    println("----------单线程方式执行一次----------")
    this.spark.sql("use tmp")
    this.spark.sql("show tables").show()
  }

  /**
    * 以单独的线程循环执行
    */
  def loop: Unit = {
    println("-----周期性执行------")
    this.spark.sql("desc ods.gd_scan_send_bag_new").show(100, false)
  }
}
