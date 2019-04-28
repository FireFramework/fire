package com.zto.bigdata.spark

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.ext.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.{GlobalConstants, SparkUtils}

/**
  * Streaming接入的例子
  *
  * @author ChengLong 2019-4-28 14:15:06
  */
object StreamingDemo extends BaseSparkStreaming {

  def main(args: Array[String]): Unit = {
    // 第一行必须调用init方法构造StreamingContext对象
    // 第一个参数表示批次时间，第二个参数表示十分checkPoint
    this.init(30, false)

    // 方式一：从配置文件获取相关信息: StreamingDemo.properties
    // 默认broker、topic、groupId等信息从该类同名的配置文件中读取，比如当前类名为StreamingDemo，那么默认会从StreamingDemo.properties中读取配置
    // 使用该方法需导入：import com.zto.bigdata.spark.common.ext.SparkExt._
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD(rdd => {
      // 将json数据解析成Senda对象对应的类型
      this.parseJson2DataFrame(rdd, classOf[Senda]).count()
    })

    // 方式二：代码中指定
    // 定制化从指定的kafka、topic读取数据，需提供groupId、kafka的broker地址、topic列表以逗号分隔
    val dstream2 = this.ssc.createDirectStream(this.kafkaParams("group.id", "指定kafka broker地址", GlobalConstants.KafkaConf.offsetLargest, false), SparkUtils.topicSplit("kafka的topic列表"))
    dstream2.foreachRDD(rdd => {
      this.parseJson2DataFrame(rdd, classOf[Senda]).count
    })

    this.ssc.startAwaitTermination()
  }
}
