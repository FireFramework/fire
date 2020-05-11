package com.zto.fire.core

import com.zto.fire.common.acc.AccumulatorManager
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.task.SchedulerManager
import com.zto.fire.common.util._
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.ext.module.{HBaseContextExt, KuduContextExt}
import com.zto.fire.core.rest.{RestfulRegister, SparkSystemRestful}
import com.zto.fire.core.task.InternalTask
import com.zto.fire.core.util.{SingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Spark通用父类
 * Created by ChengLong on 2018-03-06.
 */
trait BaseSpark extends SparkListener with BaseFire with Logging with Serializable {
  var conf: SparkConf = _
  var spark: SparkSession = _
  var sc: SparkContext = _
  var catalog: Catalog = _
  var ssc: StreamingContext = _
  var hiveContext: SQLContext = _
  var sqlContext: SQLContext = _
  var kuduContext: KuduContextExt = _
  var hbaseContext: HBaseContextExt = _
  val acc = AccumulatorManager
  var batchDuration: Long = _

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  override private[fire] final def boot: Unit = {
    this.splash
    PropUtils.load(this.appName)
    PropUtils.setProperty("spark.driver.class.name", this.className)
    if (StringUtils.isNotBlank(GlobalConstants.SparkConf.appName)) {
      this.appName = GlobalConstants.SparkConf.appName
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
    this.wrapLogInfo("<-- 完成fire框架初始化 -->")
  }

  /**
   * 生命周期方法：用于关闭SparkContext
   */
  override final def stop: Unit = {
    if (this.spark != null && this.sc != null && !this.sc.isStopped) {
      this.spark.stop()
    }
  }

  /**
   * 生命周期方法：进行fire框架的资源回收
   * 注：不允许子类覆盖
   */
  override private[fire] final def shutdown(stopGracefully: Boolean = true): Unit = {
    try {
      this.wrapLogInfo("<-- 完成用户资源回收 -->")

      if (stopGracefully) {
        if (this.sqlContext != null) this.sqlContext.clearCache
        if (this.ssc != null) {
          this.ssc.stop(true, stopGracefully)
          this.ssc = null
          this.sc = null
        }
        if (this.sc != null && !this.sc.isStopped) {
          this.sc.stop()
          this.sc = null
        }
      }

    } finally {
      super.shutdown(stopGracefully)
    }
  }

  /**
   * 构建或合并SparkConf
   * 注：不同的子类需根据需要复写该方法
   *
   * @param conf
   * 在conf基础上构建
   * @return
   * 合并后的SparkConf对象
   */
  def buildConf(conf: SparkConf = null): SparkConf

  /**
   * 构建一系列context对象
   */
  override private[fire] final def createContext(conf: Any): Unit = {
    this.restfulRegister = new RestfulRegister(this.threadPool).port(restPort)
    this.systemRestful = new SparkSystemRestful(this)

    // 注册到zrc平台，并覆盖配置信息
    if (this.jobType != JobType.SPARK_CORE) PropUtils.invokeZrcConf(this.className, s"${SystemInfoUtils.getIp}:${this.restPort}")
    PropUtils.print()
    val tmpConf = if (conf == null) this.buildConf(null) else conf.asInstanceOf[SparkConf]
    tmpConf.setAll(PropUtils.toMap)
    tmpConf.set("spark.driver.class.simple.name", this.driverClass)
    tmpConf.set("hive.metastore.uris", GlobalConstants.HiveConf.getMetastoreUrl)
    if (SystemInfoUtils.isLocal) {
      this.spark = SparkSession.builder().config(tmpConf).master("local[*]") /*.enableHiveSupport()*/ .getOrCreate()
    } else {
      this.spark = SparkSession.builder().config(tmpConf).enableHiveSupport().getOrCreate()
    }
    SingletonFactory.setSparkSession(this.spark)
    this.spark.registerUDF()
    this.sc = this.spark.sparkContext
    // 关联所连接的hive集群，根据预制方案启用HDFS HA
    GlobalConstants.HdfsConf.linkHiveCluster(this.sc.hadoopConfiguration)
    this.catalog = this.spark.catalog
    this.sc.setLogLevel(GlobalConstants.SparkConf.logLevel)
    val sparkListener = new BaseSparkListener(this)
    this.sc.addSparkListener(sparkListener)
    this.initLogging(this.className)
    this.hiveContext = this.spark.sqlContext
    this.sqlContext = this.hiveContext
    this.hbaseContext = SingletonFactory.getHBaseContextInstance(this.sc)
    this.kuduContext = SingletonFactory.getKuduContextInstance(this.sc)
    this.applicationId = SparkUtils.getApplicationId(this.spark)
    this.webUI = SparkUtils.getWebUI(this.spark)
    this.conf = tmpConf
    // 向driver和executor注册定时任务
    val taskSchedule = new InternalTask(this)
    // driver端注册定时任务
    SchedulerManager.registerTasks(this, taskSchedule, sparkListener)
    // executor端与自定义累加器一同完成定时任务注册
    AccumulatorManager.registerTasks(this, taskSchedule)
    // 向executor端注册自定义累加器
    if (this.jobType != JobType.SPARK_CORE) this.acc.registerAccumulators(this.sc)

    this.wrapLogInfo("<-- 完成Spark运行时信息初始化 -->")
  }

  /**
   * 用于注册定时任务实例
   *
   * @param instances
   * 标记有@Scheduled类的实例
   */
  def registerSchedule(instances: Object*): Unit = {
    try {
      // 向driver端注册定时任务
      SchedulerManager.registerTasks(instances: _*)
      // 向executor端注册定时任务
      val executors = this.conf.get("spark.executor.instances").toInt
      if (executors > 0 && this.sc != null) {
        this.sc.parallelize(1 to executors, executors).foreachPartition(i => SchedulerManager.registerTasks(instances: _*))
      }
    } catch {
      case e => this.log("定时任务注册失败.", e)
    }
  }
}