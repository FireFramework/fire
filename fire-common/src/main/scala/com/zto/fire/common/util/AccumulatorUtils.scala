package com.zto.fire.common.util

import java.nio.ByteBuffer

import com.zto.fire.common.acc.LogAccumulator
import com.zto.fire.common.bean.TimeCost
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.util.LongAccumulator

/**
  * fire内置累加器工具类
  * @author ChengLong 2019-7-25 19:11:16
  */
object AccumulatorUtils {
  // 累加器名称
  val logAccumulator = "logAccumulator"
  val countAccumulator = "countAccumulator"

  /**
    * 将timeCost累加到日志累加器中
    * @param timeCost
    */
  def addLogValue(timeCost: TimeCost): Unit = {
    val logAccumulator = SparkEnv.get.conf.get(this.logAccumulator, "")
    if (StringUtils.isNotBlank(logAccumulator)) {
      val log: LogAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(logAccumulator)))
      log.add(timeCost)
    }
  }

  /**
    * 将数据累加到count累加器中
    * @param value
    */
  def addCountValue(value: Long): Unit = {
    val countAccumulator = SparkEnv.get.conf.get(this.countAccumulator, "")
    if (StringUtils.isNotBlank(countAccumulator)) {
      val count: LongAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(countAccumulator)))
      count.add(value)
    }
  }
}
