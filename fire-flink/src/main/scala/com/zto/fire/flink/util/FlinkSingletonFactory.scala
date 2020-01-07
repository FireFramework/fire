package com.zto.fire.flink.util

import java.util.Properties

import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.GlobalConstants
import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkContext, SparkEnv}

/**
  * 单例工厂，用于创建单例的对象
  * Created by ChengLong on 2020年1月6日 16:50:56.
  */
object FlinkSingletonFactory {
  @transient private[this] var tableEnv: StreamTableEnvironment = _

  /**
    * 设置TableEnv实例
    */
  private[fire] def setTableEnv(tableEnv: StreamTableEnvironment): Unit = {
    if (tableEnv != null) this.tableEnv = tableEnv
  }

  /**
   * 设置TableEnv实例
   *
   * @return
   * TableEnv实例
   */
  private[fire] def getTableEnv: StreamTableEnvironment = this.tableEnv
}
