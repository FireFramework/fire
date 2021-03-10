package com.zto.fire.flink

import com.zto.fire._
import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.{OSUtils, PropUtils}
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.util.{FlinkSingletonFactory, FlinkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * flink streaming通用父接口
 *
 * @author ChengLong 2020年1月7日 10:50:19
 */
trait BaseFlinkStreaming extends BaseFlink {
  protected var env, senv, flink, fire: StreamExecutionEnvironment = _
  protected var tableEnv: StreamTableEnvironment = _
  override val jobType: JobType = JobType.FLINK_STREAMING
  // 用于存放延期的数据
  protected val outputTag = new OutputTag[Any]("later_data")


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
      PropUtils.toEngineConfMap.foreach(t => tmpConf.setString(t._1, t._2))
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
    this.process
  }

  /**
   * 初始化flink运行时环境
   */
  override def createContext(conf: Any): Unit = {
    super.createContext(conf)
    if (FlinkUtils.isYarnApplicationMode) this.restfulRegister.startRestServer
    val finalConf = this.buildConf(conf.asInstanceOf[Configuration])
    if (OSUtils.isLocal) {
      this.env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(finalConf)
    } else {
      this.env = StreamExecutionEnvironment.getExecutionEnvironment
    }
    this.env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(finalConf.toMap))
    this.configParse(this.env)
    this.senv = this.env
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    this.tableEnv = StreamTableEnvironment.create(this.env, settings)
    val tableConfig = this.tableEnv.getConfig.getConfiguration
    FireFlinkConf.flinkSqlConfig.filter(kv => noEmpty(kv, kv._1, kv._2)).foreach(kv => tableConfig.setString(kv._1, kv._2))
    if (StringUtils.isNotBlank(FireHiveConf.getHiveConfDir)) {
      this.tableEnv.registerCatalog(FireHiveConf.hiveCatalogName, this.hive)
      this.tableEnv.useCatalog(FireHiveConf.hiveCatalogName)
    }
    this.flink = this.env
    this.fire = this.flink
    FlinkSingletonFactory.setStreamEnv(this.env).setStreamTableEnv(this.tableEnv)
    FlinkUtils.loadUdfJar
    // 自动注册配置文件中指定的udf函数
    FireFlinkConf.flinkUdfList.filter(udf => noEmpty(udf, udf._1, udf._2)).foreach(udf => {
      val createFunction = s"CREATE FUNCTION ${udf._1} AS '${udf._2}'"
      this.tableEnv.executeSql(createFunction)
      logger.info(s"execute sql: $createFunction")
    })
  }

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    // 子类复写该方法实现业务处理逻辑
  }
}
