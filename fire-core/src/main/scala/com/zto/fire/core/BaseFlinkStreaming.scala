package com.zto.fire.core

import com.zto.fire.common.enu.JobType
import com.zto.fire.flink.util.FlinkSingletonFactory
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * flink streaming通用父接口
 * @author ChengLong 2020年1月7日 10:50:19
 */
trait BaseFlinkStreaming extends BaseFlink {
  var env, ssc: StreamExecutionEnvironment = _
  var tableEnv, flink: StreamTableEnvironment = _
  override val jobType: JobType = JobType.FLINK_STREAMING

  override def init(conf: Any = null, args: Array[String] = null): Unit = {
    if (conf != null) conf.asInstanceOf[Configuration].setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    this.env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf.asInstanceOf[Configuration])
    this.ssc = this.env
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    this.tableEnv = StreamTableEnvironment.create(this.env, bsSettings)

    this.env.enableCheckpointing(500)
    this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    this.flink = this.tableEnv
    FlinkSingletonFactory.setTableEnv(this.tableEnv)

    this.process
  }

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {

  }
}
