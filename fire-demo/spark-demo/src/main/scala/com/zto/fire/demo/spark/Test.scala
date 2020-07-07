package com.zto.fire.demo.spark

import java.util

import com.zto.fire.common.data.DataPool
import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.GlobalConstants
import com.zto.fire.core.{BaseSparkCore, BaseSparkStreaming}
import com.zto.fire.core.ext.SparkExt._
import org.apache.rocketmq.spark.RocketMQConfig
import org.apache.spark.sql.Encoders

object Test extends BaseSparkStreaming {

  override def process: Unit = {
    val params = new util.HashMap[String, String]()
    params.put(RocketMQConfig.MAX_PULL_SPEED_PER_PARTITION, "111")
    params.put(RocketMQConfig.NAME_SERVER_ADDR, "localhost:7890")
    params.put(RocketMQConfig.CONSUMER_TAG, "757")

    val dstream = this.ssc.createRocketPullStream(params)
  }

  def main(args: Array[String]): Unit = {
     // this.init(10, false)
    println(DataPool.getDatasource)
  }
}
