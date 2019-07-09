package com.zto.fire.core.util

import java.util.Properties

import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.GlobalConstants
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.ext.module.{HBaseContextExt, KuduContextExt}
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * 单例工厂，用于创建单例的对象
  * Created by ChengLong on 2018-04-25.
  */
object SingletonFactory {
  @transient private var sqlContext: SQLContext = _
  @transient private var hbaseContext: HBaseContextExt = _
  @transient private var kuduContext: KuduContextExt = _
  private var hbase: HBaseOper = _
  private var sparkSession: SparkSession = _

  /**
    * 获取SparkSession实例
    *
    * @return
    * SparkSession实例
    */
  def getSparkSession: SparkSession = this.sparkSession

  /**
    * SparkSession赋值
    * @param sparkSession
    */
  def setSparkSession(sparkSession: SparkSession): Unit = {
    this.sparkSession = sparkSession
  }

  /**
    * 获取HBaseOper单例对象
    *
    * @return
    * HBaseOper实例
    */
  def getHBaseOperInstance: HBaseOper = {
    if (hbase == null) {
      this.hbase = new HBaseOper
    }
    this.hbase
  }

  /**
    * 获取单例的SQLContext对象
    *
    * @param sparkContext
    * SparkContext实例
    * @param sql
    * 预执行的sql
    * @param props
    * 指定SQLContext的配置信息
    * @return
    */
  def getSQLContextInstance(sparkContext: SparkContext, sql: String = null, props: Properties = null): SQLContext = {
    if (this.sqlContext == null) {
      this.sqlContext = sparkContext.createSQLContext
      if (props != null && props.size() > 0) {
        this.sqlContext.setConf(props)
      }
      if (StringUtils.isNotBlank(sql)) {
        this.sqlContext.sql(sql)
      }
    }
    this.sqlContext
  }

  /**
    * 获取单例的HBaseContext对象
    *
    * @param sparkContext
    * SparkContext实例
    * @return
    */
  def getHBaseContextInstance(sparkContext: SparkContext): HBaseContextExt = {
    if (this.hbaseContext == null) {
      this.hbaseContext = new HBaseContextExt(sparkContext, HBaseOper.getConfiguration)
    }
    this.hbaseContext
  }

  /**
    * 获取单例的KuduContext对象
    *
    * @param sparkContext
    * SparkContext实例
    * @return
    */
  def getKuduContextInstance(sparkContext: SparkContext): KuduContextExt = {
    if (this.kuduContext == null) {
      val kuduContextTmp = new KuduContext(GlobalConstants.KuduConf.kuduMaster, sparkContext)
      this.kuduContext = new KuduContextExt(SingletonFactory.getSQLContextInstance(sparkContext), kuduContextTmp)
    }
    this.kuduContext
  }

}
