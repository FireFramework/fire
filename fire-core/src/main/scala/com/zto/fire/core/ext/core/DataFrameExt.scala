package com.zto.fire.core.ext.core

import java.util.Properties

import com.zto.fire.common.bean.{BaseLogging, HBaseBaseBean}
import com.zto.fire.common.conf.{FireJdbcConf, FireSparkConf}
import com.zto.fire.common.db.JdbcOper
import com.zto.fire.common.util.{DBUtils, ValueUtils}
import com.zto.fire.core.bridge.HBaseSparkBridge
import com.zto.fire.core.ext.module.HBaseContextExt
import com.zto.fire.core.util.{SingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.mutable.ListBuffer
import scala.reflect._

/**
 * DataFrame扩展
 *
 * @param dataFrame
 * dataFrame实例
 */
class DataFrameExt(dataFrame: DataFrame) extends BaseLogging {

  // 获取单例的HBaseContext对象
  private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(dataFrame.sparkSession.sparkContext)
  private lazy val spark: SparkSession = SingletonFactory.getSparkSession

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
  def saveAsHiveTable(tableName: String, partitionName: String, saveMode: SaveMode = SaveMode.valueOf(FireSparkConf.saveMode)): DataFrame = {
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
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   */
  def jdbcTableSave(tableName: String, saveMode: SaveMode = SaveMode.Append, jdbcProps: Properties = null, keyNum: Int = 1): Unit = {
    dataFrame.write.mode(saveMode).jdbc(FireJdbcConf.url(keyNum), tableName, DBUtils.getJdbcProps(jdbcProps, keyNum))
  }

  /**
   * 将DataFrame中指定的列写入到jdbc中
   * 调用者需自己保证DataFrame中的列类型与关系型数据库对应字段类型一致
   *
   * @param sql
   * 关系型数据库待执行的增删改sql
   * @param fields
   * 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序
   * 若不指定字段，则默认传入当前DataFrame所有列，且列的顺序与sql中问号占位符顺序一致
   * @param batch
   * 每个批次执行多少条
   * @param keyNum
   * 对应配置文件中指定的数据源编号
   */
  def jdbcBatchUpdate(sql: String, fields: Seq[String] = null, batch: Int = FireJdbcConf.batchSize(), keyNum: Int = 1): Unit = {
    if (ValueUtils.isEmpty(sql)) {
      this.log("执行jdbcBatchUpdate失败，sql语句不能为空")
      return
    }

    if (dataFrame.isStreaming) {
      // 如果是streaming流
      dataFrame.writeStream.format("fire-jdbc")
        .option("checkpointLocation", FireSparkConf.chkPointDirPrefix)
        .option("sql", sql)
        .option("batch", batch)
        .option("keyNum", keyNum)
        .option("fields", if (fields != null) fields.mkString(",") else "")
        .start()
    } else {
      // 非structured streaming调用
      dataFrame.foreachPartition(it => {
        var count: Int = 0
        val list = ListBuffer[ListBuffer[Any]]()
        var params: ListBuffer[Any] = null

        it.foreach(row => {
          count += 1
          params = ListBuffer[Any]()
          if (ValueUtils.isNotEmpty(fields)) {
            // 若调用者指定了某些列，则取这些列的数据
            fields.foreach(field => {
              val index = row.fieldIndex(field)
              params += row.get(index)
            })
          } else {
            // 否则取当前DataFrame全部的列，顺序要与sql问号占位符保持一致
            (0 until row.size).foreach(index => {
              params += row.get(index)
            })
          }
          list += params

          // 分批次执行
          if (count == batch) {
            JdbcOper.executeBatch(sql, list, keyNum = keyNum)
            count = 0
            list.clear()
          }
        })

        // 将剩余的数据一次执行掉
        if (list.nonEmpty) {
          JdbcOper.executeBatch(sql, list, keyNum = keyNum)
          list.clear()
        }
      })
    }
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
   *
   * @param clazz
   * @return
   */
  def toRDD[E <: Object : ClassTag](clazz: Class[E], toUppercase: Boolean = false): RDD[E] = {
    this.dataFrame.rdd.mapPartitions(it => SparkUtils.sparkRowToBean(it, clazz, toUppercase))
  }

  /**
   * 将DataFrame的schema转为小写
   *
   * @return
   */
  def toLowerDF: DataFrame = {
    this.dataFrame.selectExpr(SparkUtils.schemaToLowerCase(this.dataFrame.schema): _*)
  }

  /**
   * 清空RDD的缓存
   */
  def uncache: Unit = {
    dataFrame.unpersist()
  }

  /**
   * 将实时流转为静态DataFrame
   *
   * @return
   * 静态DataFrame
   */
  def toExternalRow: DataFrame = {
    if (this.dataFrame.isStreaming) SparkUtils.toExternalRow(dataFrame) else this.dataFrame
  }
}