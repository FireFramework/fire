package com.zto.fire.core

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.PropUtils

/**
  * Structured Streaming通用父类
  * Created by ChengLong on 2019-03-11.
  */
class BaseStructuredStreaming extends BaseSpark {
  override val jobType = JobType.SPARK_STRUCTURED_STREAMING

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param conf
    * Spark配置信息
    * @param args main方法参数
    */
  override def init(conf: Any = null, args: Array[String] = null): Unit = {
    super.init(conf, args)
    // 添加时间监听器
    this.spark.streams.addListener(new BaseStreamingQueryListener)
    this.restfulRegister.startRestServer
    this.process
  }

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {}


  /**
   * 在加载任务配置文件前将被加载
   */
  override private[fire] def loadConf: Unit = {
    PropUtils.load(FireFrameworkConf.SPARK_STRUCTURED_STREAMING_CONF_FILE)
  }
}
