package com.zto.fire.common.acc

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger

import com.zto.fire.common.bean.TimeCost
import com.zto.fire.common.util.{StringsUtils, SystemInfoUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkContext, SparkEnv}

/**
  * fire内置累加器工具类
  *
  * @author ChengLong 2019-7-25 19:11:16
  */
private[fire] object AccumulatorManager {
  // 累加器名称，含有fire的名字将会显示在webui中
  private[this] val counterLabel = "fire-counter"
  private[fire] val counter = new LongAccumulator

  private[this] val logAccumulatorLabel = "logAccumulator"
  private[fire] val logAccumulator = new LogAccumulator

  private[this] val accMap = Map(this.logAccumulatorLabel -> this.logAccumulator, this.counterLabel -> this.counter)
  private[fire] val executorInstances: AtomicInteger = new AtomicInteger(0)
  private[this] val initExecutors: AtomicInteger = new AtomicInteger(0)

  /**
    * 将数据累加到count累加器中
    *
    * @param value
    * 累加值
    */
  def addCounter(value: Long): Unit = {
    if (SparkEnv.get != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
      val countAccumulator = SparkEnv.get.conf.get(this.counterLabel, "")
      if (StringUtils.isNotBlank(countAccumulator)) {
        val counter: LongAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(countAccumulator)))
        counter.add(value)
      }
    } else {
      this.counter.add(value)
    }
  }

  /**
    * 获取counter累加器的值
    *
    * @return
    * 累加结果
    */
  def getCounter: Long = this.counter.value

  /**
    * 将timeCost累加到日志累加器中
    *
    * @param timeCost
    * TimeCost实例对象
    */
  def addLog(timeCost: TimeCost): Unit = {
    val env = SparkEnv.get
    if (env != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
      val logAccumulator = SparkEnv.get.conf.get(this.logAccumulatorLabel, "")
      if (StringUtils.isNotBlank(logAccumulator)) {
        val logAcc: LogAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(logAccumulator)))
        logAcc.add(timeCost)
      }
    } else {
      this.logAccumulator.add(timeCost)
    }
  }

  /**
    * 获取日志累加器中的值
    *
    * @return
    * 日志累加值
    */
  def getLog: ConcurrentLinkedQueue[String] = this.logAccumulator.value

  /**
    * 注册多个自定义累加器到每个executor
    *
    * @param sc
    * SparkContext
    * [key, accumulator]
    */
  private[fire] def registerAccumulators(sc: SparkContext): Unit = {
    if (sc != null && accMap != null && accMap.size > 0) {
      if (this.initExecutors.get() == 0) this.initExecutors.set(sc.getConf.get("spark.executor.instances", if (SystemInfoUtils.isLinux) "10000" else "10").toInt)
      if (this.initExecutors.get() > this.executorInstances.get()) this.executorInstances.set(this.initExecutors.get())

      val accumulatorMap = accMap.map(accInfo => {
        // 注册每个累加器，必须是合法的名称并且未被注册过
        if (accInfo._2 != null && !accInfo._2.isRegistered) {
          if (StringUtils.isNotBlank(accInfo._1) && accInfo._1.contains("fire")) {
            sc.register(accInfo._2, accInfo._1)
          } else {
            sc.register(accInfo._2)
          }
        }
        (accInfo._1, SparkEnv.get.closureSerializer.newInstance().serialize(accInfo._2).array())
      })

      // 获取申请的executor数，设置累加器到conf中
      val rdd = sc.parallelize(1 to this.executorInstances.get, this.executorInstances.get)
      rdd.foreachPartition(i => {
        // 将序列化后的累加器放置到conf中
        accumulatorMap.foreach(accSer => SparkEnv.get.conf.set(accSer._1, StringsUtils.toHexString(accSer._2)))
      })
    }
  }
}
