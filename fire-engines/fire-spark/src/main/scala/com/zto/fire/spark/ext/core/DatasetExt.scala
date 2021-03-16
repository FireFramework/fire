package com.zto.fire.spark.ext.core

import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.spark.connector.{HBaseBulkConnector, HBaseSparkBridge}
import com.zto.fire.spark.util.SparkUtils
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger

import scala.collection.mutable.ListBuffer
import scala.reflect._

/**
 * Dataset扩展
 *
 * @param dataset
 * dataset对象
 * @author ChengLong 2019-5-18 11:02:56
 */
class DatasetExt[T: ClassTag](dataset: Dataset[T]) {

  /**
   * 用于检查当前Dataset是否为空
   *
   * @return
   * true: 为空 false：不为空
   */
  def isEmpty: Boolean = dataset.rdd.isEmpty()

  /**
   * 用于检查当前Dataset是否不为空
   *
   * @return
   * true: 不为空 false：为空
   */
  def isNotEmpty: Boolean = !this.isEmpty

  /**
   * 打印Dataset的值
   *
   * @param lines
   * 打印的行数
   * @return
   */
  def showString(lines: Int = 1000): String = {
    val showLines = if (lines <= 1000) lines else 1000
    val showStringMethod = dataset.getClass.getDeclaredMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
    showStringMethod.invoke(dataset, Integer.valueOf(showLines), Integer.valueOf(Int.MaxValue), java.lang.Boolean.valueOf(false)).toString
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def hbaseBulkPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.bulkPutDS[T](tableName, dataset.asInstanceOf[Dataset[T]], keyNum)
  }

  /**
   * 根据Dataset[String]批量删除，Dataset是rowkey的集合
   * 类型为String
   *
   * @param tableName
   * HBase表名
   */
  def hbaseBulkDeleteDS(tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.bulkDeleteDS(tableName, dataset.asInstanceOf[Dataset[String]], keyNum)
  }

  /**
   * 根据Dataset[RowKey]批量删除记录
   *
   * @param tableName
   * rowKey集合
   */
  def hbaseDeleteDS(tableName: String, keyNum: Int = 1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbaseDeleteDS(tableName, dataset.asInstanceOf[Dataset[String]])
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   */
  def hbaseHadoopPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.hadoopPutDS[T](tableName, dataset.asInstanceOf[Dataset[T]], keyNum)
  }

  /**
   * 使用Java API的方式将Dataset中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   */
  def hbasePutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], keyNum: Int = 1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutDS[E](tableName, clazz, dataset.asInstanceOf[Dataset[E]])
  }

  /**
   * 清空RDD的缓存
   */
  def uncache: Unit = {
    dataset.unpersist
  }

  /**
   * 将当前Dataset记录打印到控制台
   */
  def print(outputMode: String = "append", trigger: Trigger = null, numRows: Int = 20, truncate: Boolean = true): Dataset[T] = {
    if (dataset.isStreaming) {
      val tmpStream = dataset.writeStream.outputMode(outputMode).option("numRows", numRows).option("truncate", truncate).format("console")
      if (trigger != null) tmpStream.trigger(trigger)
      tmpStream.start
    } else {
      dataset.show(numRows, truncate)
    }
    dataset
  }

  /**
   * 分配次执行指定的业务逻辑
   *
   * @param batch
   *            多大批次执行一次sinkFun中定义的操作
   * @param mapFun
   *            将Row类型映射为E类型的逻辑，并将处理后的数据放到listBuffer中
   * @param sinkFun
   * 具体处理逻辑，将数据sink到目标源
   */
  def foreachPartitionBatch[E](mapFun: T => E, sinkFun: ListBuffer[E] => Unit, batch: Int = 1000): Unit = {
    SparkUtils.datasetForeachPartitionBatch(this.dataset, mapFun, sinkFun, batch)
  }

}