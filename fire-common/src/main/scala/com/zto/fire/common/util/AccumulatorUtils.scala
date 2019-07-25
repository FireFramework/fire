package com.zto.fire.common.util

import java.nio.ByteBuffer

import com.zto.fire.common.acc.LogAccumulator
import com.zto.fire.common.bean.TimeCost
import org.apache.spark.SparkEnv
import org.apache.spark.util.LongAccumulator

/**
  * fire内置累加器工具类
  * @author ChengLong 2019-7-25 19:11:16
  */
object AccumulatorUtils {

  /**
    * 将timeCost累加到日志累加器中
    * @param timeCost
    */
  def addLogValue(timeCost: TimeCost): Unit = {
    val logAccumulator: LogAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(SparkEnv.get.conf.get("logAccumulator"))))
    logAccumulator.add(timeCost)
  }

  /**
    * 将数据累加到count累加器中
    * @param value
    */
  def addCountValue(value: Long): Unit = {
    val count: LongAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(SparkEnv.get.conf.get("countAccumulator"))))
    count.add(value)
  }
}
