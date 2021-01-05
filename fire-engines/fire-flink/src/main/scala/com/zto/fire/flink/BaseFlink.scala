package com.zto.fire.flink

import com.zto.fire._
import com.zto.fire.common.conf.{FireFlinkConf, FireFrameworkConf, FireHiveConf, FireSparkConf}
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.{PropUtils, OSUtils}
import com.zto.fire.core.BaseFire
import com.zto.fire.core.rest.RestfulRegister
import com.zto.fire.flink.rest.FlinkSystemRestful
import com.zto.fire.flink.task.FlinkSchedulerManager
import com.zto.fire.flink.util.{FlinkSingletonFactory, FlinkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.catalog.hive.HiveCatalog


/**
 * Flink引擎通用父接口
 *
 * @author ChengLong 2020年1月7日 09:31:09
 */
trait BaseFlink extends BaseFire {
  protected var conf: Configuration = _
  protected var hive: HiveCatalog = _

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  override private[fire] def boot: Unit = {
    PropUtils.compatible("flink")
    PropUtils.load(FireFrameworkConf.FLINK_CONF_FILE)
    this.loadConf
    PropUtils.load(this.appName)
    PropUtils.setProperty(FireFlinkConf.FLINK_DRIVER_CLASS_NAME, this.className)
    PropUtils.setProperty(FireFlinkConf.FLINK_CLIENT_SIMPLE_CLASS_NAME, this.driverClass)
    FlinkSingletonFactory.setAppName(this.appName)
    super.boot
  }

  /**
   * 初始化flink运行时环境
   */
  override private[fire] def createContext(conf: Any): Unit = {
    retry(FireFrameworkConf.restfulPortRetryNum, FireFrameworkConf.restfulPortRetryDuration) {
      this.restPort = OSUtils.getRundomPort(FireFrameworkConf.restPortRandomBound)
      this.restfulRegister = new RestfulRegister(this.threadPool).port(restPort)
    }
    this.systemRestful = new FlinkSystemRestful(this, this.restfulRegister)
    val restAddress = s"${OSUtils.getIp}:${this.restPort}"
    PropUtils.setProperty(FireFrameworkConf.fireRestUrl(PropUtils.engine), s"http://$restAddress")

    // 注册到实时平台，并覆盖配置信息
    if (this.jobType == JobType.FLINK_STREAMING) PropUtils.invokeConfigCenter(this.className, restAddress)
    PropUtils.print()
    FlinkSchedulerManager.getInstance().registerTasks(this)
    // 创建HiveCatalog
    val hiveConfDir = FireHiveConf.getHiveConfDir
    if (StringUtils.isNotBlank(hiveConfDir)) {
      this.logger.info("enabled flink-hive support.")
      this.logger.info(s"hive-site.xml path is $hiveConfDir")
      this.hive = new HiveCatalog(FireHiveConf.hiveCatalogName, FireSparkConf.defaultDB, hiveConfDir, FireHiveConf.hiveVersion)
    }
  }

  /**
   * 构建或合并Configuration
   * 注：不同的子类需根据需要复写该方法
   *
   * @param conf
   * 在conf基础上构建
   * @return
   * 合并后的Configuration对象
   */
  def buildConf(conf: Configuration): Configuration

  /**
   * 生命周期方法：用于回收资源
   */
  override def stop: Unit = {
    try {
      this.after()
    } finally {
      this.shutdown()
    }
  }

  /**
   * 生命周期方法：进行fire框架的资源回收
   * 注：不允许子类覆盖
   */
  override private[fire] def shutdown(stopGracefully: Boolean = true): Unit = {
    super.shutdown(stopGracefully)
    System.exit(0)
  }

  /**
   * 用于解析configuration中的配置，识别flink参数（非用户自定义参数），并设置到env中
   */
  private[fire] def configParse(env: Any): ExecutionConfig = {
    requireNonEmpty(env)("Environment对象不能为空")
    val config = if (env.isInstanceOf[ExecutionEnvironment]) {
      val batchEnv = env.asInstanceOf[ExecutionEnvironment]
      // flink.default.parallelism
      if (FireFlinkConf.defaultParallelism != -1) batchEnv.setParallelism(FireFlinkConf.defaultParallelism)
      batchEnv.getConfig
    } else {
      val streamEnv = env.asInstanceOf[StreamExecutionEnvironment]
      // flink.max.parallelism
      if (FireFlinkConf.maxParallelism != -1) streamEnv.setMaxParallelism(FireFlinkConf.maxParallelism)
      // flink.default.parallelism
      if (FireFlinkConf.defaultParallelism != -1) streamEnv.setParallelism(FireFlinkConf.defaultParallelism)
      // flink.stream.buffer.timeout.millis
      if (FireFlinkConf.streamBufferTimeoutMillis != -1) streamEnv.setBufferTimeout(FireFlinkConf.streamBufferTimeoutMillis)
      // flink.stream.number.execution.retries
      if (FireFlinkConf.streamNumberExecutionRetries != -1) streamEnv.setNumberOfExecutionRetries(FireFlinkConf.streamNumberExecutionRetries)
      // flink.stream.time.characteristic
      if (StringUtils.isNotBlank(FireFlinkConf.streamTimeCharacteristic)) streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(FireFlinkConf.streamTimeCharacteristic))

      // checkPoint相关参数
      val ckConfig = streamEnv.getCheckpointConfig
      if (ckConfig != null && FireFlinkConf.streamCheckpointInterval != -1) {
        // flink.stream.checkpoint.interval 单位：毫秒 默认：-1 关闭
        streamEnv.enableCheckpointing(FireFlinkConf.streamCheckpointInterval)
        // flink.stream.checkpoint.mode  EXACTLY_ONCE/AT_LEAST_ONCE 默认：EXACTLY_ONCE
        if (StringUtils.isNotBlank(FireFlinkConf.streamCheckpointMode)) ckConfig.setCheckpointingMode(CheckpointingMode.valueOf(FireFlinkConf.streamCheckpointMode.trim.toUpperCase))
        // flink.stream.checkpoint.timeout 单位：毫秒 默认：10 * 60 * 1000
        if (FireFlinkConf.streamCheckpointTimeout != null) ckConfig.setCheckpointTimeout(FireFlinkConf.streamCheckpointTimeout)
        // flink.stream.checkpoint.max.concurrent 默认：1
        if (FireFlinkConf.streamCheckpointMaxConcurrent > 0) ckConfig.setMaxConcurrentCheckpoints(FireFlinkConf.streamCheckpointMaxConcurrent)
        // flink.stream.checkpoint.min.pause.between  默认：0
        if (FireFlinkConf.streamCheckpointMinPauseBetween >= 0) ckConfig.setMinPauseBetweenCheckpoints(FireFlinkConf.streamCheckpointMinPauseBetween)
        // flink.stream.checkpoint.prefer.recovery  默认：false
        ckConfig.setPreferCheckpointForRecovery(FireFlinkConf.streamCheckpointPreferRecovery)
        // flink.stream.checkpoint.tolerable.failure.number 默认：0
        if (FireFlinkConf.streamCheckpointTolerableTailureNumber >= 0) ckConfig.setTolerableCheckpointFailureNumber(FireFlinkConf.streamCheckpointTolerableTailureNumber)
        // flink.stream.checkpoint.externalized
        if (StringUtils.isNotBlank(FireFlinkConf.streamCheckpointExternalized)) ckConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.valueOf(FireFlinkConf.streamCheckpointExternalized.trim))
      }

      streamEnv.getConfig
    }
    FlinkUtils.parseConf(config)

    config
  }
}
