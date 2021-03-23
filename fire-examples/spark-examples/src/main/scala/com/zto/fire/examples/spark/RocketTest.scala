package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.spark.BaseSparkStreaming

object RocketTest extends BaseSparkStreaming {
  override def process: Unit = {
    //读取RocketMQ消息流
    val dStream = this.fire.createRocketMqPullStream()
    dStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val source = rdd.map(msgExt =>  new String(msgExt.getBody).replace("messageBody", ""))
        import fire.implicits._
        this.fire.read.json(source.toDS()).createOrReplaceTempView("tmp_scanrecord")
        this.fire.sql(
          """
            |select *
            |from tmp_scanrecord
            |""".stripMargin).show(10,false)
      }
    })

    dStream.rocketCommitOffsets
    this.fire.start()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
