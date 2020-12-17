package com.zto.fire.core.util

import com.zto.fire.common.conf.{FireFrameworkConf, FireHBaseConf, FireKuduConf}
import com.zto.fire.common.db.v2.HBaseOper
import com.zto.fire.common.enu.JobType
import com.zto.fire.core.ext.module.{HBaseContextExt, KuduContextExt}
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkContext, SparkEnv}

/**
 * 单例工厂，用于创建单例的对象
 * Created by ChengLong on 2018-04-25.
 */
object SingletonFactory {
  @transient private var hbaseContext: HBaseContextExt = _
  @transient private var kuduContext: KuduContextExt = _
  private[fire] var sparkSession: SparkSession = _
  private var jobClassName: String = _
  private[fire] var jobType: JobType = JobType.UNDEFINED

  /**
   * 获取任务的类型
   */
  def getJobType: JobType = this.jobType

  /**
   * 获取SparkSession实例
   *
   * @return
   * SparkSession实例
   */
  def getSparkSession: SparkSession = this.sparkSession

  /**
   * SparkSession赋值
   */
  def setSparkSession(sparkSession: SparkSession): Unit = this.synchronized {
    this.sparkSession = sparkSession
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
  def getHBaseContextInstance(sparkContext: SparkContext, keyNum: Int = 1): HBaseContextExt = this.synchronized {
    if (this.hbaseContext == null && StringUtils.isNotBlank(FireHBaseConf.hbaseCluster())) {
      this.hbaseContext = new HBaseContextExt(sparkContext, HBaseOper.getConfiguration(keyNum))
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
      this.kuduContext = new KuduContextExt(SingletonFactory.sparkSession.sqlContext, kuduContextTmp)
    }
    this.kuduContext
  }

}
