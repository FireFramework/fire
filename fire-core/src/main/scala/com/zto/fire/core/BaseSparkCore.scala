package com.zto.fire.core

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.PropUtils
import org.apache.spark.SparkConf

/**
  * 实时平台Spark通用父类
  * Created by ChengLong on 2018-03-28.
  */
class BaseSparkCore extends BaseSpark {
  override val jobType = JobType.SPARK_CORE

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param conf
    * Spark配置信息
    */
  override def init(conf: Any = null, args: Array[String] = null): Unit = {
    super.init(conf, args)
    this.process
  }

  /**
   * 在加载任务配置文件前将被加载
   */
  override private[fire] def loadConf: Unit = {
    PropUtils.load(FireFrameworkConf.SPARK_CORE_CONF_FILE)
  }

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {}
}
