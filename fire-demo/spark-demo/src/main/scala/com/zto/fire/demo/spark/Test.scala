package com.zto.fire.demo.spark

import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.GlobalConstants
import com.zto.fire.core.{BaseSparkCore, BaseSparkStreaming}
import com.zto.fire.core.ext.SparkExt._
import org.apache.spark.sql.Encoders

object Test extends BaseSparkStreaming {

  override def process: Unit = {
    val param = Map[String, Object]("bootstrap.servers" -> "localhost:9092", "group.id" -> "fire1")
    val dstream = this.ssc.createDirectStream(param, topics = Set("fire1", "flink1"), groupId = "fire2")
    dstream.print()
    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // this.init(10, false)
    println(GlobalConstants.RocketConf.rocketNameServer())
  }
}
