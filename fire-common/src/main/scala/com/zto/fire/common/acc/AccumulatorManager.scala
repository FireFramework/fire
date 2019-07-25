package com.zto.fire.common.acc

import java.nio.ByteBuffer

import com.zto.fire.common.bean.TimeCost
import com.zto.fire.common.util.StringsUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkContext, SparkEnv}

/**
  * fire内置累加器工具类
  *
  * @author ChengLong 2019-7-25 19:11:16
  */
object AccumulatorManager {
  // 累加器名称，含有fire的名字将会显示在webui中
  val log = "logAccumulator"
  val counter = "fire-counter"
  var executorInstances: Int = 0

  /**
    * 将timeCost累加到日志累加器中
    *
    * @param timeCost
    */
  def addLogValue(timeCost: TimeCost): Unit = {
    val logAccumulator = SparkEnv.get.conf.get(this.log, "")
    if (StringUtils.isNotBlank(logAccumulator)) {
      val log: LogAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(logAccumulator)))
      log.add(timeCost)
    }
  }

  /**
    * 将数据累加到count累加器中
    *
    * @param value
    */
  def addCountValue(value: Long): Unit = {
    val countAccumulator = SparkEnv.get.conf.get(this.counter, "")
    if (StringUtils.isNotBlank(countAccumulator)) {
      val count: LongAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(countAccumulator)))
      count.add(value)
    }
  }

  /**
    * 注册多个自定义累加器到每个executor
    *
    * @param sc
    * SparkContext
    * @param accumulatorInfo
    * [key, accumulator]
    */
  private[fire] def registerAccumulators(sc: SparkContext, accumulatorInfo: Map[String, AccumulatorV2[_, _]]): Unit = {
    if (sc != null && accumulatorInfo != null && accumulatorInfo.size > 0) {
      val executors = sc.getConf.get("spark.executor.instances", "10000").toInt
      if (executors > this.executorInstances) {
        // 获取申请的executor数，设置累加器到conf中
        val rdd = sc.parallelize(1 to executors, executors)
        val accumulatorMap = accumulatorInfo.map(accInfo => {
          // 注册每个累加器，必须是合法的名称
          if (accInfo._2 != null && !accInfo._2.isRegistered) {
            println("开始注册：" + accInfo._1 + " " + accInfo._2)
            if (StringUtils.isNotBlank(accInfo._1) && accInfo._1.contains("fire")) {
              sc.register(accInfo._2, accInfo._1)
            } else {
              sc.register(accInfo._2)
            }
          }
          (accInfo._1, SparkEnv.get.closureSerializer.newInstance().serialize(accInfo._2).array())
        })
        rdd.foreachPartition(i => {
          // 将序列化后的累加器放置到conf中
          accumulatorMap.foreach(accSer => SparkEnv.get.conf.set(accSer._1, StringsUtils.toHexString(accSer._2)))
        })
        this.executorInstances = executors
      }
    }
  }
}
