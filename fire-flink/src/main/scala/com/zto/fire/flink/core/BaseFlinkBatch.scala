package com.zto.fire.flink.core

import com.zto.fire.common.conf.{FireFlinkConf, FireFrameworkConf, FireHiveConf}
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.{PropUtils, SystemInfoUtils}
import com.zto.fire.flink.core.util.{FlinkSingletonFactory, FlinkUtils}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.table.api.scala.BatchTableEnvironment

/**
 * flink batch通用父接口
 * @author ChengLong 2020年1月7日 15:15:56
 */
trait BaseFlinkBatch extends BaseFlink {
  override val jobType: JobType = JobType.FLINK_BATCH
  protected var env, sc: ExecutionEnvironment = _
  protected var tableEnv, flink: BatchTableEnvironment = _

  /**
   * 构建或合并Configuration
   * 注：不同的子类需根据需要复写该方法
   *
   * @param conf
   * 在conf基础上构建
   * @return
   * 合并后的Configuration对象
   */
  override def buildConf(conf: Configuration): Configuration = {
    val finalConf = if (conf != null) conf else {
      val tmpConf = new Configuration()
      PropUtils.toFlinkConfMap.foreach(t => tmpConf.setString(t._1, t._2))
      tmpConf
    }
    finalConf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)

    this.conf = finalConf
    finalConf
  }


  /**
   * 程序初始化方法，用于初始化必要的值
   *
   * @param conf
   * 用户指定的配置信息
   * @param args
   * main方法参数列表
   */
  override def init(conf: Any = null, args: Array[String] = null): Unit = {
    super.init(conf, args)
    if (conf != null) conf.asInstanceOf[Configuration].setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)

    this.process
  }

  /**
   * 创建计算引擎运行时环境
   *
   * @param conf
   * 配置信息
   */
  override private[fire] def createContext(conf: Any): Unit = {
    super.createContext(conf)
    val finalConf = this.buildConf(conf.asInstanceOf[Configuration])
    if (SystemInfoUtils.isLocal) {
      this.env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(finalConf)
    } else {
      this.env = ExecutionEnvironment.getExecutionEnvironment
    }
    this.env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(finalConf.toMap))
    this.configParse(this.env)
    this.sc = this.env
    this.tableEnv = BatchTableEnvironment.create(this.env)
    if (FireHiveConf.hiveSupportEnable) {
      this.tableEnv.registerCatalog(FireHiveConf.hiveCatalogName, this.hive)
      this.tableEnv.useCatalog(FireHiveConf.hiveCatalogName)
    }
    this.flink = this.tableEnv
    FlinkSingletonFactory.setEnv(this.env).setTableEnv(this.tableEnv)
    this.deployConf
  }

  /**
   * 用于fire框架初始化，传递累加器与配置信息到taskManager端
   */
  override protected def deployConf: Unit = {
    this.sc.fromCollection(1 to this.sc.getParallelism)
      .map(FlinkUtils.initMapFunction)
      .setParallelism(this.sc.getParallelism)
      .name("fire init")
  }

  /**
   * 在加载任务配置文件前将被加载
   */
  override private[fire] def loadConf: Unit = {
    PropUtils.load(FireFrameworkConf.FLINK_BATCH_CONF_FILE)
    PropUtils.setProperty(FireFlinkConf.FLINK_FIRE_CONFIGURATION, FireFrameworkConf.FLINK_BATCH_CONF_FILE)
  }

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {

  }
}
