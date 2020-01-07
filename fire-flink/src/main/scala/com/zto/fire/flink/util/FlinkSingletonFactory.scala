package com.zto.fire.flink.util

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * 单例工厂，用于创建单例的对象
  * Created by ChengLong on 2020年1月6日 16:50:56.
  */
object FlinkSingletonFactory {
  @transient private[this] var streamEnv: StreamExecutionEnvironment = _
  @transient private[this] var streamTableEnv: StreamTableEnvironment = _

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
  private[fire] def setStreamTableEnv(tableEnv: StreamTableEnvironment): this.type = {
    if (tableEnv != null && this.streamTableEnv == null) this.streamTableEnv = tableEnv
    this
  }

  /**
   * 设置TableEnv实例
   *
   * @return
   * TableEnv实例
   */
  private[fire] def getStreamTableEnv: StreamTableEnvironment = this.streamTableEnv
}
