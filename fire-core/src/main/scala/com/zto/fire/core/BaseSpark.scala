package com.zto.fire.core

import com.zto.fire.common.acc.AccumulatorManager
import com.zto.fire.common.conf.{FireFrameworkConf, FireHDFSConf, FireHiveConf, FireSparkConf}
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.task.SchedulerManager
import com.zto.fire.common.util._
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.ext.module.{HBaseContextExt, KuduContextExt}
import com.zto.fire.core.rest.{RestfulRegister, SparkSystemRestful}
import com.zto.fire.core.task.SparkInternalTask
import com.zto.fire.core.util.{SingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
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
  private[fire] var _conf: SparkConf = _
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
  var listener: SparkListener = _

  /**
   * 获取配置信息
   */
  def conf = this.acc.getConf

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  override private[fire] final def boot: Unit = {
    this.loadConf
    PropUtils.load(this.appName)
    PropUtils.setProperty("spark.driver.class.name", this.className)
    if (StringUtils.isNotBlank(FireSparkConf.appName)) {
      this.appName = FireSparkConf.appName
    }
    super.boot
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
  def buildConf(conf: SparkConf): SparkConf = {
    if (conf == null) new SparkConf().setAppName(this.appName) else conf
  }


  /**
   * 构建一系列context对象
   */
  override private[fire] final def createContext(conf: Any): Unit = {
    this.retry(FireFrameworkConf.restfulPortRetryNum, FireFrameworkConf.restfulPortRetryDuration) {
      this.restPort = SystemInfoUtils.getRundomPort
      this.restfulRegister = new RestfulRegister(this.threadPool).port(restPort)
    }
    this.systemRestful = new SparkSystemRestful(this)

    // 注册到zrc平台，并覆盖配置信息
    if (this.jobType != JobType.SPARK_CORE && FireFrameworkConf.zrcEnable) PropUtils.invokeZrcConf(this.className, s"${SystemInfoUtils.getIp}:${this.restPort}")
    PropUtils.print()

    // 构建SparkConf信息
    val tmpConf = if (conf == null) this.buildConf(null) else conf.asInstanceOf[SparkConf]
    tmpConf.setAll(PropUtils.toMap)
    tmpConf.set("spark.driver.class.simple.name", this.driverClass)

    // 如果启用hive，则获取hive metastore地址
    if (FireHiveConf.hiveSupportEnable) {
      val hiveMetastoreUrl = FireHiveConf.getMetastoreUrl
      assert(StringUtils.isNotBlank(hiveMetastoreUrl), "未找到匹配的hive metastore地址，请配置：spark.hive.cluster=xxx或通过spark.hive.support.enable=false禁用hive.")
      tmpConf.set("hive.metastore.uris", hiveMetastoreUrl)
    }

    // 构建SparkSession对象
    val sessionBuilder = SparkSession.builder().config(tmpConf)
    // spark.hive.support.enable
    if (FireHiveConf.hiveSupportEnable) sessionBuilder.enableHiveSupport()
    // 在mac或windows环境下执行local模式，cpu数通过spark.local.cores指定，默认local[*]
    if (SystemInfoUtils.isLocal) sessionBuilder.master(s"local[${FireSparkConf.localCores}]")
    this.spark = sessionBuilder.getOrCreate()

    SingletonFactory.setSparkSession(this.spark)
    this.spark.registerUDF()
    this.sc = this.spark.sparkContext
    // 关联所连接的hive集群，根据预制方案启用HDFS HA
    FireHDFSConf.linkHiveCluster(this.sc.hadoopConfiguration)
    this.catalog = this.spark.catalog
    this.sc.setLogLevel(FireSparkConf.logLevel)
    this.listener = new BaseSparkListener(this)
    this.sc.addSparkListener(listener)
    this.initLogging(this.className)
    this.hiveContext = this.spark.sqlContext
    this.sqlContext = this.hiveContext
    this.hbaseContext = SingletonFactory.getHBaseContextInstance(this.sc)
    this.kuduContext = SingletonFactory.getKuduContextInstance(this.sc)
    this.applicationId = SparkUtils.getApplicationId(this.spark)
    this.webUI = SparkUtils.getWebUI(this.spark)
    this._conf = tmpConf
    this.deployConf
    this.wrapLogInfo("<-- 完成Spark运行时信息初始化 -->")
    SparkUtils.executeHiveConfSQL(this.spark)
  }

  /**
   * 用于fire框架初始化，传递累加器与配置信息到taskManager端
   */
  override protected def deployConf: Unit = {
    // 向driver和executor注册定时任务
    val taskSchedule = new SparkInternalTask(this)
    // driver端注册定时任务
    SchedulerManager.registerTasks(this, taskSchedule, this.listener)
    // executor端与自定义累加器一同完成定时任务注册
    AccumulatorManager.registerTasks(this, taskSchedule)
    // 向executor端注册自定义累加器
    if (FireFrameworkConf.accEnable) this.acc.registerAccumulators(this.sc)
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
      val executors = this._conf.get("spark.executor.instances").toInt
      if (executors > 0 && this.sc != null) {
        this.sc.parallelize(1 to executors, executors).foreachPartition(i => SchedulerManager.registerTasks(instances: _*))
      }
    } catch {
      case e => this.log("定时任务注册失败.", e)
    }
  }
}