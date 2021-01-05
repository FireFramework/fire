package com.zto.fire.spark.util

import com.zto.fire.common.conf.{FireFrameworkConf, FireHBaseConf, FireKuduConf}
import com.zto.fire.common.enu.JobType
import com.zto.fire.core.util.SingletonFactory
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.connector.HBaseBulkConnector
import com.zto.fire.spark.ext.module.KuduContextExt
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkContext, SparkEnv}

/**
 * 单例工厂，用于创建单例的对象
 * Created by ChengLong on 2018-04-25.
 */
object SparkSingletonFactory extends SingletonFactory {
  private[this] var sparkSession: SparkSession = _
  private[this] var streamingContext: StreamingContext = _
  @transient private[this] var hbaseContext: HBaseBulkConnector = _
  @transient private[this] var kuduContext: KuduContextExt = _
  private var jobClassName: String = _

  /**
   * 获取SparkSession实例
   *
   * @return
   * SparkSession实例
   */
  def getSparkSession: SparkSession = this.synchronized {
    this.sparkSession
  }

  /**
   * SparkSession赋值
   */
  private[fire] def setSparkSession(sparkSession: SparkSession): Unit = this.synchronized {
    require(sparkSession != null, "SparkSession实例不能为空")
    this.sparkSession = sparkSession
  }

  /**
   * 设置StreamingContext
   * 允许重复赋值，兼容热重启导致的StreamingContext重新被创建
   */
  private[fire] def setStreamingContext(ssc: StreamingContext): Unit = this.synchronized {
    require(ssc != null, "StreamingContext实例不能为空")
    this.streamingContext = ssc
  }

  /**
   * 获取StreamingContext实例
   */
  def getStreamingContext: StreamingContext = this.synchronized {
    this.streamingContext
  }

  /**
   * 用于获取当前的job全类名
   */
  def getJobClassName: String = this.synchronized {
    if (StringUtils.isBlank(this.jobClassName)) {
      this.jobClassName = SparkEnv.get.conf.get(FireFrameworkConf.DRIVER_CLASS_NAME, "")
    }
    this.jobClassName
  }


  /**
   * 获取单例的HBaseContext对象
   *
   * @param sparkContext
   * SparkContext实例
   * @return
   */
  def getHBaseContextInstance(sparkContext: SparkContext, keyNum: Int = 1): HBaseBulkConnector = this.synchronized {
    if (this.hbaseContext == null && StringUtils.isNotBlank(FireHBaseConf.hbaseCluster())) {
      this.hbaseContext = new HBaseBulkConnector(sparkContext, HBaseConnector.getConfiguration(keyNum))
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
  def getKuduContextInstance(sparkContext: SparkContext): KuduContextExt = this.synchronized {
    if (this.kuduContext == null && StringUtils.isNotBlank(FireKuduConf.kuduMaster)) {
      val kuduContextTmp = new KuduContext(FireKuduConf.kuduMaster, sparkContext)
      this.kuduContext = new KuduContextExt(this.sparkSession.sqlContext, kuduContextTmp)
    }
    this.kuduContext
  }

}
