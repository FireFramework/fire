package com.zto.bigdata.spark.common.ext.core

import java.util.Properties

import com.zto.bigdata.spark.common.bean.HBaseBaseBean
import com.zto.bigdata.spark.common.db.HBaseSparkBridge
import com.zto.bigdata.spark.common.ext.module.HBaseContextExt
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect._

/**
  * DataFrame扩展
  *
  * @param dataFrame
  * dataFrame实例
  */
class DataFrameExt(dataFrame: DataFrame) {

  // 获取单例的HBaseContext对象
  private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(dataFrame.sparkSession.sparkContext)

  /**
    * 注册为临时表的同时缓存表
    *
    * @param tmpTableName
    * 临时表名
    * @return
    * 生成的DataFrame
    */
  def createOrReplaceTempViewCache(tmpTableName: String): DataFrame = {
    if (StringUtils.isNotBlank(tmpTableName)) {
      dataFrame.createOrReplaceTempView(tmpTableName)
      dataFrame.sqlContext.cacheTable(tmpTableName)
    }
    dataFrame
  }

  /**
    * 保存Hive表
    *
    * @param saveMode
    * 保存模式，默认为Overwrite
    * @param partitionName
    * 分区字段
    * @param tableName
    * 表名
    * @return
    * 生成的DataFrame
    */
  def saveAsHiveTable(tableName: String, partitionName: String, saveMode: SaveMode = GlobalConstants.SparkConf.saveMode): DataFrame = {
    if (StringUtils.isNotBlank(tableName)) {
      if (StringUtils.isNotBlank(partitionName)) {
        dataFrame.write.mode(saveMode).partitionBy(partitionName).saveAsTable(tableName)
      } else {
        dataFrame.write.mode(saveMode).saveAsTable(tableName)
      }
    }
    dataFrame
  }

  /**
    * 将DataFrame数据保存到关系型数据库中
    *
    * @param tableName
    * 关系型数据库表名
    * @return
    */
  def jdbcTableSave(tableName: String, saveMode: SaveMode = SaveMode.Append, jdbcProps: Properties = null, keyNum: Int = 1): Unit = {
    dataFrame.write.mode(saveMode).jdbc(GlobalConstants.JdbcConf.url(keyNum), tableName, SparkUtils.getJdbcProps(jdbcProps, keyNum))
  }

  /**
    * 批量写入，将自定义的JavaBean数据集批量并行写入
    * 到HBase的指定表中。内部会将自定义JavaBean的相应
    * 字段一一映射为Put对象，并完成一次写入
    *
    * @param tableName
    * HBase表名
    * @param insertEmpty
    * 对象中值为空的字段是否覆盖HBase中已有的field值
    * 默认为覆盖
    * @tparam T
    * 数据类型为HBaseBaseBean的子类
    */
  def hbaseBulkPutDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    this.hbaseContext.bulkPutDF[T](tableName, dataFrame, clazz, insertEmpty, multiVersion)
  }

  /**
    * 以spark 方式批量将DataFrame数据写入到hbase中
    * 注：此方法与hbaseHadoopPutDF不同之处在于，它不强制要求该DataFrame一定要与HBaseBaseBean的子类对应
    * 但需要指定rowKey的构建规则，相对与hbaseHadoopPutDF来说，少了中间的两次转换，性能会更高
    *
    * @param tableName
    * hbase表名
    * @param insertEmpty
    * 为空的字段是否写入hbase
    * @tparam T
    * JavaBean类型
    */
  def hbaseHadoopPutDFRow[T <: HBaseBaseBean[T] : ClassTag](tableName: String, buildRowKey: (Row) => String, insertEmpty: Boolean = true): Unit = {
    this.hbaseContext.hadoopPutDFRow[T](tableName, dataFrame, buildRowKey, insertEmpty)
  }


  /**
    * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * JavaBean类型，为HBaseBaseBean的子类
    */
  def hbaseHadoopPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], insertEmpty: Boolean = true): Unit = {
    this.hbaseContext.hadoopPutDF[E](tableName, dataFrame, clazz, insertEmpty)
  }


  /*
    /**
      * 批量load数据到hbase
      *
      * @param tableName
      * HBase表名
      * @param stagingDir
      * 临时路径
      * @param insertEmpty
      * 是否将为空的字段写入到HBase
      * @tparam T
      */
    def hbaseBulkLoadThinRows[T <: HBaseBaseBean[T] : ClassTag](tableName: String,
                                                                stagingDir: String, insertEmpty: Boolean = true): Unit = {
      this.hbaseContext.bulkLoadThinRows(tableName, rdd, stagingDir, insertEmpty)
    }*/




  /**
    * 使用Java API的方式将DataFrame中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * JavaBean类型，为HBaseBaseBean的子类
    * @param batchSize
    * 批次大小
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    */
  def hbaseOperPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    HBaseSparkBridge.hbaseOperPutDF(tableName, this.dataFrame, clazz, insertEmpty, batchSize, multiVersion)
  }

  /**
    * 将DataFrame注册为临时表，并缓存表
    *
    * @param tableName
    * 临时表名
    */
  def dataFrameRegisterAndCache(tableName: String): Unit = {
    if (StringUtils.isBlank(tableName)) throw new IllegalArgumentException("临时表名不能为空")
    dataFrame.createOrReplaceTempView(tableName)
    dataFrame.sqlContext.cacheTable(tableName)
  }

  /**
    * 将DataFrame映射为指定JavaBean类型的RDD
    * @param clazz
    * @return
    */
  def toRDD[E <: Object : ClassTag](clazz: Class[E]): RDD[E] = {
    this.dataFrame.rdd.mapPartitions(it => SparkUtils.sparkRowToBean(it, clazz))
  }

  /**
    * 清空RDD的缓存
    */
  def uncache: Unit = {
    dataFrame.unpersist()
  }
}