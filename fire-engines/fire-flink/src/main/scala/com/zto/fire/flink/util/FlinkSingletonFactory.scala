package com.zto.fire.flink.util

import com.zto.fire.common.util.ValueUtils
import com.zto.fire.core.util.SingletonFactory
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}

/**
  * 单例工厂，用于创建单例的对象
  * Created by ChengLong on 2020年1月6日 16:50:56.
  */
object FlinkSingletonFactory extends SingletonFactory {
  @transient private[this] var appName: String = _
  @transient private[this] var streamEnv: StreamExecutionEnvironment = _
  @transient private[this] var streamTableEnv: StreamTableEnvironment = _
  @transient private[this] var env: ExecutionEnvironment = _
  @transient private[this] var tableEnv: BatchTableEnvironment = _

  /**
   * 设置TableEnv实例
   */
  private[fire] def setStreamEnv(env: StreamExecutionEnvironment): this.type = {
    if (env != null && this.streamEnv == null) this.streamEnv = env
    this
  }

  /**
   * 设置TableEnv实例
   */
  private[fire] def setAppName(appName: String): this.type = {
    if (ValueUtils.noEmpty(appName) && ValueUtils.isEmpty(this.appName)) this.appName = appName
    this
  }


  /**
    * 设置TableEnv实例
    */
  private[fire] def setStreamTableEnv(tableEnv: StreamTableEnvironment): this.type = {
    if (tableEnv != null && this.streamTableEnv == null) this.streamTableEnv = tableEnv
    this
  }

  /**
   * 设置ExecutionEnvironment实例
   */
  private[fire] def setEnv(env: ExecutionEnvironment): this.type = {
    if (env != null && this.env == null) this.env = env
    this
  }


  /**
    * 设置TableEnv实例
    */
  private[fire] def setTableEnv(tableEnv: BatchTableEnvironment): this.type = {
    if (tableEnv != null && this.tableEnv == null) this.tableEnv = tableEnv
    this
  }

  /**
   * 获取appName
   *
   * @return
   * TableEnv实例
   */
  private[fire] def getAppName: String = this.appName

  /**
   * 获取StreamTableEnv实例
   *
   * @return
   * TableEnv实例
   */
  private[fire] def getStreamTableEnv: StreamTableEnvironment = {
    require(this.streamTableEnv != null, "StreamTableEnvironment仍未被实例化，请稍后再试")
    this.streamTableEnv
  }

  /**
   * 获取TableEnv实例
   *
   * @return
   * TableEnv实例
   */
  private[fire] def getBatchTableEnv: BatchTableEnvironment = this.tableEnv
}
