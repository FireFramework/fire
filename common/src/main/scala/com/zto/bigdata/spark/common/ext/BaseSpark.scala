package com.zto.bigdata.spark.common.ext

import java.util.concurrent.Executors

import com.zto.bigdata.spark.common.rest.{RestfulRegister, SystemRestful}
import com.zto.bigdata.spark.common.util.{DateFormatUtils, GlobalConstants, LogUtils, SparkUtils}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Spark通用父类
  * Created by ChengLong on 2018-03-06.
  */
trait BaseSpark extends SparkListener with Serializable {
  var conf: SparkConf = _
  var spark: SparkSession = _
  var sc: SparkContext = _
  var hiveContext: SQLContext = _
  var sqlContext: SQLContext = _
  var kuduContext: KuduContextExt = _
  var hbaseContext: HBaseContextExt = _
  val startTime = SparkUtils.currentTime
  var appName = this.getClass.getSimpleName.replace("$", "")
  val threadPool = Executors.newFixedThreadPool(20)
  val restfulRegister = new RestfulRegister(this.threadPool)
  private val systemRestful = new SystemRestful(this)
  val log = LoggerFactory.getLogger(this.getClass)
  val logger = new LogUtils(log)

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param appName
    * job名称
    * @param conf
    * Spark配置信息
    */
  def init(beanDir: String = "", appName: String = "", conf: SparkConf = null): Unit

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  def process: Unit

  /**
    * 打印总耗时
    *
    * @param applicationEnd
    * 整个job执行结束后执行
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (this.hiveContext != null) this.hiveContext.clearCache
    GlobalConstants.PrintModule.END_TIME_COST(this.startTime)
  }

  /**
    * 以子线程方式执行函数调用
    *
    * @param fun
    * 用于指定以多线程方式执行的函数
    */
  def runAsThread(fun: => Unit, debug: Boolean = false): Unit = {
    this.threadPool.execute(new Runnable {
      override def run(): Unit = {
        fun
        if(debug) println(s"--> ${GlobalConstants.Color.GREEN}Invoke ${fun.getClass.getName} as thread. Time: ${DateFormatUtils.formatCurrentDateTime()}. ${GlobalConstants.Color.DEFAULT}<--")
      }
    })
  }

  /**
    * 以子线程方式循环执行函数调用
    *
    * @param fun
    * 用于指定以多线程方式执行的函数
    * @param delay
    * 循环调用间隔时间（单位s）
    */
  def runAsThreadLoop(fun: => Unit, delay: Long = 10, debug: Boolean = false): Unit = {
    this.threadPool.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          if(debug) println(s"--> ${GlobalConstants.Color.GREEN}Loop invoke ${fun.getClass.getName} as thread. Delay is ${delay}s. Time: ${DateFormatUtils.formatCurrentDateTime()}. ${GlobalConstants.Color.DEFAULT}<--")
          fun
          Thread.sleep(delay * 1000)
        }
      }
    })
  }


}